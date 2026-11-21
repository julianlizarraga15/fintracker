# Close the 20 Nov security findings

This run changes the repo so the findings that are code can be closed without breaking the next deploy. Six changes land in the working tree, in the order below. Four of them edit `backend/tests/test_http_workflows.py`. The limiter reset lands first, then the backend units one after another. This run does not open pull requests and does not commit.

An independent review of the first draft dropped the file-backed login limiter. A restart is controlled by the operator, and a new source address still gets its own window. The review also kept Nginx delivery and secret rotation with the operator.

These stay with you.

- Place the reviewed `nginx.conf` on the host as described in `docs/shared-proxy.md`. Run `nginx -t`, reload the proxy, then `curl -sI https://fintrackerapp.xyz/`. The web deploy does not publish this file. `ci-cd.yml` does not trigger on it, and the host bind-mounts its own copy.
- Create the AWS role and stop using the long-lived GitHub access keys. Removing the keys before the role exists breaks deploy.
- Turn on the approval rule for the `production` environment. The workflow names that environment so the rule has a job to gate. GitHub creates the environment with no rules, so the next deploy still runs until you add the rule.
- Create the admin IAM user with MFA, and stop using root. See `docs/plans/20261025-aws-admin-identity.md`.
- Rotate the login password and `JWT_SECRET` together in `/fintracker/web`, then restart only the backend. See `docs/plans/20261025-ec2-remaining-exposure.md`. A high-entropy password is the brake a new address cannot skip.
- Decide whether to rotate the Etherscan and Blockcypher keys.
- Move the backend image off root only after the host data directory is writable by that user.
- Read the instance security group.

## Landing order

1. `reset-login-limiter`
2. `close-api-docs`
3. `add-browser-headers`
4. `hide-server-paths`
5. `neutralize-job-date`
6. `bind-token-to-password`
7. `pin-action-shas`

`reset-login-limiter` is a fixture step inside the backend tests. It is not a separate product change. `add-browser-headers` is in the repo, and the live header check stays with you.

## reset-login-limiter

- [ ] Depends on nothing.
- [ ] Files. `backend/tests/test_http_workflows.py`.
- [ ] Change. The `client` fixture replaces `_attempts_by_client` with an empty `OrderedDict` before each test.
- [ ] Result. Login attempts from one test do not consume the window of the next test.
- [ ] Unit. The existing login tests still pass when several of them run in one process.
- [ ] Live. The fixture is the test client. There is no production behavior.
- [ ] Perf. No production change.

## close-api-docs

- [ ] Depends on `reset-login-limiter` only because they share the test module.
- [ ] Files. `backend/app/main.py`, `backend/tests/test_http_workflows.py`.
- [ ] Change. Construct `FastAPI` with `docs_url=None`, `redoc_url=None`, and `openapi_url=None`.
- [ ] Result. `GET /docs`, `GET /redoc`, and `GET /openapi.json` return 404. `GET /health` still returns 200.
- [ ] Unit. `pytest backend/tests/test_http_workflows.py -k docs`.
- [ ] Live. One unauthenticated `GET` of `https://fintrackerapp.xyz/api/openapi.json` returns 404 after the image that contains this change is deployed. This run does not deploy.
- [ ] Perf. No new per-request work.

## add-browser-headers

- [ ] Depends on nothing.
- [ ] Files. `nginx.conf`, `backend/Dockerfile.test`, `backend/tests/test_nginx_headers.py`.
- [ ] Change. On the `listen 443 ssl default_server` block only, add four `add_header` lines with `always`. The headers are `Strict-Transport-Security` with `max-age=15552000`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Content-Security-Policy: frame-ancestors 'none'`. Do not set them on the watering-system server. Do not set `script-src` or `includeSubDomains`. The test image copies `nginx.conf` so the test can read it. CI's backend image does not otherwise contain that file.
- [ ] Result. The Fintracker HTTPS server sends the four headers on 401 and 404 responses as well as 200. The watering server is unchanged.
- [ ] Unit. `pytest backend/tests/test_nginx_headers.py`.
- [ ] Live. You run this after placing the file on the host. `curl -sI` shows the four headers. `nginx -t` cannot pass on this machine because the certificate paths are absent.
- [ ] Perf. The headers are static.

## hide-server-paths

- [ ] Depends on `reset-login-limiter` for the HTTP test only.
- [ ] Files. `backend/app/job_runs.py`, `backend/app/valuations.py`, `backend/app/manual_crypto.py`, `backend/tests/test_job_runs.py`, `backend/tests/test_valuations.py`, `backend/tests/test_http_workflows.py`.
- [ ] Change. `get_latest_job_run` returns file names for `log_file` and for each `outputs` path, including records already stored with directories. HTTP errors from a missing valuation file, a bad job record, and an unreadable manual holdings file do not include a directory. `source_file` is the file name. The log parser still keeps the paths it extracted. Those assertions stay.
- [ ] Result. A valid token can load a job and a valuation. The JSON and the error text do not contain a directory or `account=`.
- [ ] Unit. The new job, valuation, and HTTP assertions.
- [ ] Live. The test client is the app. This run does not call production.
- [ ] Perf. Name trimming only.

## neutralize-job-date

- [ ] Depends on nothing.
- [ ] Files. `frontend/jobs.html`, `e2e/dashboard.spec.js`.
- [ ] Change. `formatDateTime` returns a fixed placeholder when `Date` cannot parse the value. The history cell passes that result through `escapeHtml`.
- [ ] Result. A history row whose `started_at` is `<img src=x onerror=alert(1)>` shows the placeholder and no `img` element.
- [ ] Unit. The Playwright test routes that history payload and asserts both.
- [ ] Live. The Playwright page is the jobs page. This machine may not start Chromium. CI's browser job is the run that executes the test.
- [ ] Perf. One date parse per row.

## bind-token-to-password

- [ ] Depends on `reset-login-limiter`.
- [ ] Files. `backend/app/security.py`, `backend/tests/test_http_workflows.py`.
- [ ] Change. The issued token gains a `pwd` claim. The claim is HMAC-SHA256 of the current password, keyed by `JWT_SECRET`, hex encoded. `require_jwt` rejects a token whose claim does not match the current password. A token with no `pwd` claim is rejected after the signature and expiry checks. The stored password stays plaintext. Replacing it with a hash would lock login out until SSM is rewritten.
- [ ] Result. A token from login works. After `DEMO_AUTH_PASSWORD` changes and `JWT_SECRET` stays the same, that token returns 401 with `Invalid token.` An expired token still returns `Token expired.`
- [ ] Unit. `test_password_change_invalidates_existing_token` and the existing expired-token test.
- [ ] Live. The test client is the app. This run does not log into production.
- [ ] Perf. Time 30 authenticated `GET /manual/crypto-holdings` calls with one token, before the claim check and after it. Fail if the median rises by more than 25 ms.

## pin-action-shas

- [ ] Depends on nothing.
- [ ] Files. `.github/workflows/ci-cd.yml`, `.github/workflows/tests.yml`, `.github/workflows/broker-lambdas.yml`, `.github/workflows/broker-isolation-audit.yml`.
- [ ] Change. Replace each third-party `uses` tag with the commit returned by `GET /repos/{owner}/{repo}/commits/{tag}`. Keep the tag in a trailing comment. Leave in-repo `uses: ./.github/workflows/...` unchanged. Do not remove the AWS access keys. On the deploy job, set `environment: production`.
- [ ] Result. No workflow `uses` line ends in `@v4`, `@v5`, `@v2`, or `@v3`. The deploy job names `environment: production`.
- [ ] Unit. Search `.github/workflows` for `uses:` lines with a floating tag.
- [ ] Live. Do not run the deploy workflow.
- [ ] Perf. No runtime change.

## Checked on 20 Nov 2026

Local backend suite passed, 126 tests. That covers the docs routes, the header file, the path redaction, the password claim, and the limiter reset. The holdings GET median over 30 calls was 2.378 ms before the HMAC check and 2.743 ms after. The fail line was 25 ms. A Node call of `formatDateTime` from `frontend/jobs.html` returned the placeholder for an `<img>` string, and the history cell calls `escapeHtml`. Playwright is not installed here, so `e2e/dashboard.spec.js` did not run. Docker was not running, so the test image was not built. No workflow was dispatched, and the host Nginx file was not reloaded.

## What remains open after this run

On 21 Nov 2026 the host Nginx file was reloaded, public TCP 8000 was revoked, and `fintracker-admin` was confirmed. The workflows assume `github-actions-fintracker`. Still open: delete the old access key after one successful role deploy, add the GitHub approval rule, rotate the login password and `JWT_SECRET`, and leave the container user as root while data files are root-owned.
