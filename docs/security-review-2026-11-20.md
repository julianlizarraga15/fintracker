# Security review — 2026-11-20

Source review of the repository as of this date. Each claim was checked in a separate pass against the current tree. The OpenAPI result also includes an unauthenticated GET of the public host. `.env` and `.isolation-bootstrap` were not read. No live AWS account inventory was taken.

This is a record of findings. It does not change production.

## Result

One high finding, two medium findings, and five low findings held. Two claims did not hold. Broker-key rejection, the valuation account check, and HS256-only token verification still hold.

## Findings

### High — production deploy identity

`.github/workflows/ci-cd.yml` authenticates with long-lived `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, not GitHub OIDC. It calls `aws-actions/configure-aws-credentials@v4`, `aws-actions/amazon-ecr-login@v2`, and `actions/checkout@v4` by mutable tag. The workflow file sets no GitHub Environment approval. A qualifying push to `main`, and `workflow_dispatch` from `main`, sends `AWS-RunShellScript` to instance `i-0543a3b0c146f3cbb` and pipes that commit's `scripts/deploy_isolated_web.sh` into bash. `.github/workflows/broker-lambdas.yml` uses the same keys and the same mutable tags.

The IAM policy of that GitHub user is not in the repo. The proven power is push to the named web image repos and a shell of the deploy script on that one instance. This review does not establish that the key is an administrator. Branch protection outside the workflow file was not checked. The remote command is not fed by a pull-request title or a dispatch string; a commit that lands on `main` does control the script body.

Fix this first: pin those actions to commits, and switch the workflow to a short-lived role that can only push the two web image repos and send the existing deploy command.

### Medium — shared password and token lifetime

Login compares one `DEMO_AUTH_PASSWORD` from the environment or `/fintracker/web` in plaintext. `backend/app/login_rate_limit.py` allows 10 attempts per 60 seconds per client key, in one process's memory. A restart or a new address starts over. Nginx has no `limit_req`. Issued tokens carry `sub`, `exp`, and `iat` only. `require_jwt` accepts any unexpired HS256 token signed with `JWT_SECRET`, including after `DEMO_AUTH_PASSWORD` changes. The default lifetime is 15 minutes, including when `JWT_EXPIRES_MINUTES` is missing or not positive.

On Python 3.12, a second process was allowed to attempt a client key that the first process had already blocked.

This matches the open rotation in `docs/plans/20261025-ec2-remaining-exposure.md`. Rotating the password without rotating `JWT_SECRET` leaves already issued tokens valid until they expire.

### Medium — web secrets still on the EC2

`WEB_SSM_NAMES` in `backend/core/settings.py` still includes `JWT_SECRET`, `DEMO_AUTH_USERNAME`, `DEMO_AUTH_PASSWORD`, `ETHERSCAN_API_KEY`, and `BLOCKCYPHER_API_KEY`, and the loader writes them into the process environment. If any broker credential name in `BROKER_CREDENTIAL_NAMES` is non-empty, `load_settings` raises before the app serves. `docs/plans/20261025-aws-admin-identity.md` still marks a dedicated admin user with MFA as pending. `docs/plans/20261025-ec2-remaining-exposure.md` still describes what a host compromise reaches. No Terraform or CloudFormation in the repo creates that admin user. This pass did not observe whether the live account still uses root.

### Low — public API schema

`backend/app/main.py` constructs FastAPI without disabling docs. `nginx.conf` proxies `/api/` to the backend with no auth on that location. Unauthenticated GETs of `https://fintrackerapp.xyz/api/docs`, `/api/redoc`, and `/api/openapi.json` returned 200. `/openapi.json` on the host root returned nginx 404. The HTML pages request `/openapi.json`, which is not proxied, so the interactive pages do not load the schema. The schema URL under `/api/` does. Portfolio routes still require a bearer token.

### Low — missing browser headers

The public server in `nginx.conf` and the frontend image set no `Strict-Transport-Security`, `Content-Security-Policy`, `X-Frame-Options`, or `X-Content-Type-Options`. Port 80 only redirects. Clickjacking of Refresh or holdings save was not shown: the token is stored in `sessionStorage`, login sets no cookie, and a cross-site frame does not receive that storage.

### Low — filesystem paths to an authenticated caller

A valid token can receive local paths. Job JSON includes `log_file`. A missing valuation file for the configured account includes the account directory in the error. A different account id is rejected with "Account access denied." before that lookup.

### Low — jobs history date sink

`frontend/jobs.html` inserts `formatDateTime(row.started_at)` into `innerHTML`. If the value is not a parseable date, the function returns it unchanged, so it can carry markup. Normal runs write an ISO timestamp. Reaching this needs a job record whose `started_at` was already altered. Snapshot prices and unknown source names checked in the same pass are escaped or numeric.

### Low — backend image runs as root

`backend/Dockerfile` sets no `USER`. The command listens on `0.0.0.0:8000`. `docker-compose.isolated.yml` does not publish that port. `docker-compose.proxy.yml` publishes 80 and 443 on nginx only. `PROJECT_STATUS.md` records that a direct public TCP connection to port 8000 was refused and that the security group was not queried. This review did not query it either.

## Claims that did not hold

A wrong-length username or password does not make login return 500. On Python 3.12, `hmac.compare_digest` returns false for unequal lengths, and login takes the 401 path.

A `.env` file does not override SSM in the deployed process. `load_settings` still calls `load_dotenv`, and a variable already in the environment beats SSM unless `SSM_ENV_OVERRIDE` is set. `scripts/deploy_isolated_web.sh` writes `PYTHON_DOTENV_DISABLED=1` into the compose bootstrap, so the deployed process does not read a `.env` file. That bootstrap does not contain the login secret. `PROJECT_STATUS.md` is right that dotenv is disabled for the release path; the source call remains.

## Boundaries that held

Web startup aborts if a broker API key or password is present in the environment. Valuation reads compare the requested account id with the configured account before opening files. JWT verification allows only HS256. Nginx sets `X-Real-IP` from the client socket. The login limiter uses that header only when the peer address resolves as `fintracker-nginx`. That proxy rule was read in this review; it did not get its own falsification pass.

## Still open

The plans already recorded, and still accurate:

- Replace daily AWS root access with a dedicated admin identity and MFA (`docs/plans/20261025-aws-admin-identity.md`).
- Rotate the web login password and `JWT_SECRET` together, then confirm old sessions stop (`docs/plans/20261025-ec2-remaining-exposure.md`).
- Decide separately whether to rotate the Etherscan and Blockcypher keys.

New from this review, in order:

1. Replace the long-lived deploy keys with a short-lived role, pin the third-party actions, and add an approval gate before production SSM.
2. Keep the password rotation above tied to `JWT_SECRET`, and treat the in-memory per-address limit as the only online brake until something stronger exists.
3. Turn off the public docs routes, or stop proxying them, and add HSTS on the HTTPS server.

## Remediation in the working tree

The plan is `docs/plans/20261120-security-remediation.md`. An independent review of that plan dropped a durable login-attempt file, limited the new headers to the Fintracker HTTPS server, and left the host Nginx reload with the operator.

Landed locally, not deployed:

- `backend/app/main.py` disables `/docs`, `/redoc`, and `/openapi.json`.
- `nginx.conf` adds HSTS, `nosniff`, `DENY`, and `frame-ancestors 'none'` on the Fintracker 443 server, with `always` so error responses keep them. The watering-system server is unchanged. The web workflow does not ship this file.
- Job, valuation, and manual-holdings responses no longer include a directory.
- `frontend/jobs.html` renders an unparseable `started_at` as a placeholder, escaped.
- A token carries an HMAC of the current password. Changing `DEMO_AUTH_PASSWORD` rejects tokens that were issued before the change. `JWT_SECRET` can stay the same. The stored password is still plaintext.
- Third-party actions are pinned to the commits their tags resolved to on 20 Nov 2026. The deploy job sets `environment: production`. The workflows assume `github-actions-fintracker` instead of the long-lived keys.

Local backend suite: 126 passed. Holdings GET median, 30 calls, one token: 2.378 ms before the HMAC check and 2.743 ms after. The jobs browser test is in `e2e/dashboard.spec.js` and was not run here because Playwright is not installed.

On 21 Nov 2026 the host `nginx.conf` was replaced and the proxy container was recreated on the same pinned image so the read-only file mount picked up the new inode. Public responses from `https://fintrackerapp.xyz/` include the four headers. The watering server does not. Inbound TCP 8000 was removed from `sg-0021e2165890b4878`. `fintracker-admin` already had MFA, no access keys, and a deny on worker parameter reads.

Still open after that: delete the unused `github-actions-fintracker` access key once a deploy with the role succeeds, add the GitHub environment approval rule, rotate the login password and `JWT_SECRET`, and keep the container user as root. The data directory has thousands of root-owned files, and the host deploy user also writes there.
