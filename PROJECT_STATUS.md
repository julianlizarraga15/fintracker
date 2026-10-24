# Fintracker broker credential isolation

Updated 2026-10-24. **Isolation is incomplete.** Worker image hardening is verified. Production web cutover is blocked on administrative AWS authentication.

## Goal and guardrails

Remove reusable IOL/PPI credentials from the public EC2 backend while preserving valuations, authenticated Refresh, scheduled runs, snapshots, history and manual holdings. Never read any `.env`, disclose secrets or place them in Git, PRs, images, logs or shell arguments. Keep web CI/CD disabled. Routine work and deployment are already authorized. Delete or rotate old credentials only after the replacement and application independence are proven. Keep one owner for production IAM and cutover.

## Verified in this continuation

- GitHub `ci-cd` remains `disabled_manually`. Main is `417471ec54b1aadaf51cd793fff3cf44a700a722`, the merge of PR 20. Work is on `fix/broker-worker-image-hardening`.
- SSH reached the existing EC2 instance. Its backend host checkout is still `b061876`; `fintracker-backend` still uses the existing backend image and `/fintracker/prod`. A fresh subprocess reported no broker credentials in its startup environment. This does **not** establish absence from the long-running app, which may load them from SSM.
- `valuations.timer` is active. `valuations.service` executes `docker exec fintracker-backend /app/scripts/run_valuations.sh`. The 2026-10-24 16:00 UTC run exited 0; this alone does not establish individual source success. Next timer execution is 2026-10-25 16:00 UTC.
- Parameter metadata confirms 22 web settings under `/fintracker/web`, two IOL parameters and two PPI parameters under their worker prefixes, all `SecureString`. No values were retrieved by this audit.
- The old worker digest still has 12 HIGH, 20 MEDIUM, 5 LOW and 1 UNDEFINED finding in live ECR. The private repository has immutable tags and scan-on-push.
- Hardening commits `83dd335` and `51405e3` update all installed OS packages from pinned AL2023 release `2023.12.20261021`, record RPM versions and enforce a fail-closed scan of the exact pushed digest. The gate rejects missing scan metadata, digest mismatch, HIGH and CRITICAL findings. Bounded retries handle ECR scan registration. Independent review executed 12 gate cases and retry boundary cases.
- Build [40113111498](https://github.com/julianlizarraga15/fintracker/actions/runs/40113111498) is **successful**. Selected candidate digest is `sha256:ab9bdf00f06e9d1e06dceed3acad7aa3fdd280b8cf84d1e539d2f0aee4e3e21b`. The real image smoke check passed; ECR scan is `COMPLETE`, with severity counts `{}` and no findings. Durable evidence is [the scan summary](docs/verification/20261024-broker-image.json). Basic ECR scanning does not establish Python dependency vulnerability coverage. The candidate is **not deployed**.
- First build [40112620118](https://github.com/julianlizarraga15/fintracker/actions/runs/40112620118) passed build and smoke but queried ECR before scan registration and failed closed. Its later live scan also had zero findings. The corrected second workflow above verifies the complete path.
- The build inventory confirms OpenSSL `3.5.8-1.amzn2023.0.3`, RPM `4.16.1.3-29.amzn2023.0.8`, PCRE2 `10.40-1.amzn2023.0.4` and curl `8.21.0-5.amzn2023.0.2`.
- `PYTHON_DOTENV_DISABLED=1 PATH="$PWD/.venv/bin:$PATH" bash scripts/run_tests.sh` passed all 90 backend tests with nine existing deprecation warnings.
- No production IAM, Lambda configuration, parameter values, backend process, timer or data was changed in this continuation. No `.env` was read. No credentials were deleted or rotated.

## Current access blocker

Local administrative AWS credentials expired. The attempted CLI login timed out without completion; its old URL is expired. A new login must be started when the operator is available. Prefer a limited deployment identity over root. Do not broaden the EC2 runtime role to perform administration.

An alternative read-only audit used existing Actions credentials without exporting keys. [Run 40113285190](https://github.com/julianlizarraga15/fintracker/actions/runs/40113285190) identifies `github-actions-fintracker`; it is denied `lambda:GetAccountSettings`, `cloudformation:DescribeStackResources` and `iam:GetRole`. SSM metadata succeeded. The audit workflow deliberately attempts independent categories and fails visibly when denied. Its artifact is partial, not a completed effective-permission review. The workflow can be rerun through `broker-lambdas.yml` on this branch with `audit_only=true`; it skips building and deploying.

## Prior deployment evidence, recheck before mutation

- Stack `fintracker-broker-sources` was `CREATE_COMPLETE`. Separate IOL/PPI roles read only their own two credential parameters, with logs retained 14 days. There were no function URLs, event triggers or Lambda resource policies.
- Both workers previously passed real read-only invocations. IOL returned 7 positions/7 prices; PPI returned 10/10. These reads were **not repeated in this continuation**. Invocation tails were captured but not independently inspected. Handler source suppresses dependency stdout, stderr and Python logging and returns fixed errors.
- Last confirmed deployed worker digest is `sha256:4519969d80ca9ed5fd853939ef667d4353b3ddf334d7cefe29b5bb69033faade`. Preserve it for rollback. The web invokes unqualified function ARNs, so publishing a version alone does not redirect its calls.
- Last confirmed Lambda account concurrency was 10. Request for 1001 was `CASE_OPENED`, case `179285424800881`. No fresh administrative quota result is available. Workers were deployed without reserved concurrency. After applied quota reaches at least 102, reserve one slot per reader through the stack.
- Last administrative IAM review found `AmazonSSMReadOnlyAccess`, `AmazonSSMManagedInstanceCore` and an inline `/fintracker/*` read/path/write policy on `fintracker-ec2-role`. Its invoke policy allowed only existing Binance in `sa-east-1`. This is a known isolation gap, not a completed boundary.
- Prior prepared IAM simulations incorrectly tested some IAM, STS, ECR, CloudFormation and SSM mutation actions against Lambda ARNs. Those denials are not valid evidence. Repeat with actual role, repository, stack, instance and document ARNs, then simulate the final applied role.
- Binance role trust/log permissions were reviewed previously. Its deployed code, key restrictions and sanitized invocation tails remain unverified. Earlier duplicated Binance SSM credentials were already deleted; do not repeat that deletion.
- Prior preparation copied allowlisted web settings without changing values and recorded manual-file hashes privately. Do not assume temporary files are durable evidence. Preserve account identity, auth, source enablement, data paths, snapshots and manual holdings.

## Remaining release sequence

1. Establish administrative AWS authentication. Recheck stack, exact function digests, quota, every attached/inline policy, boundaries, resource policies and alternate credential routes. Complete Binance review. Repeat IAM simulations with correct resources.
2. Review a CloudFormation change set for the clean candidate digest. Add reserved concurrency only if applied quota supports it. Keep the prior working digest. Invoke each worker read-only; verify schema, counts, log sanitization and private equivalence.
3. Prepare a reviewed credential-free backend image independently of the disabled web CI/CD. EC2 currently has roughly 375 MiB available RAM, no swap and 6.2 GiB free disk; avoid an unconstrained host build. Preserve unrelated `nginx.conf.before-watering`.
4. Record a fresh baseline and recovery copy of manual holdings, history and snapshots. Pause timer, drain active jobs and stop the old backend. Replace bootstrap without reading `.env`, using only clean nonsecret startup settings after approved values are in `/fintracker/web`.
5. With backend stopped, install exact web parameter and three reader-invoke grants, remove broad SSM access including parameter reads in the managed agent policy, preserve agent/ECR/S3 needs and verify the applied identity. Keep old broker parameters intact and inaccessible until equivalence passes.
6. Recreate only the backend with the reviewed image and existing data mount. Verify actual startup guard, recreated process identity and approved settings. A fresh subprocess alone is insufficient proof of cleared process memory.
7. Run authenticated Refresh and the systemd scheduled path. Require success for each enabled source, expected counts, persisted snapshots/history/price history and intact editable manual holdings. Do not print financial data. Re-enable the timer after verification. Keep CI/CD disabled.
8. Remove obsolete IOL/PPI credential copies and accessible historical copies only after the replacement and absence of dependencies are proven. Recreate long-lived processes as needed. Decide rotation from the exposure assessment, then verify workers with replacement credentials.

After IAM narrowing, rollback must keep the credential-free web and restore a working worker digest. Preserve good snapshots and pause affected valuations rather than restoring broker secrets to the web or overwriting complete snapshots with unchecked partial results.

Technical plan and runbook are `docs/plans/20261024-broker-credential-isolation.md` and `docs/broker-credential-isolation.md`. Decision trail is `.audit/credential-isolation.tsv`; local checklist is `.audit/credential-isolation.todo.md`.
