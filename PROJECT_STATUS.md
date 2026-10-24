# Fintracker broker credential isolation

Updated 2026-10-24. **Production cutover complete and verified.** Public web Refresh uses `/fintracker/web` plus three read-only broker readers; the EC2 role cannot read broker credentials or deploy/inspect those readers. Web CI/CD remains manually disabled.

## Completed outcome

- Workers use image digest `sha256:ab9bdf00f06e9d1e06dceed3acad7aa3fdd280b8cf84d1e539d2f0aee4e3e21b`, built from pinned AL2023 release `2023.12.20261021`. The exact-digest ECR basic scan completed with zero findings; the image smoke check passed. This scan does not establish Python dependency vulnerability coverage. Evidence: [image verification](docs/verification/20261024-broker-image.json), [build 40113111498](https://github.com/julianlizarraga15/fintracker/actions/runs/40113111498).
- CloudFormation stack `fintracker-broker-sources` is `UPDATE_COMPLETE`; IOL and PPI reader functions each reserve concurrency 1, with account quota 1000. Post-update real reads passed: IOL 7 positions/7 prices; PPI 10/10. Both invocation tails contained only system log records. Safe published rollback versions: IOL 1, PPI 1, Binance 2. The old worker image digest remains in ECR for rollback.
- Backend runs the reviewed credential-free image built from source `070cbf5` (image ID `sha256:99979f8cbb83c1efae0358e3230315fb8475fc362090401651d12446db2899b6`). It uses `/fintracker/web`, disables dotenv, and mounts only persistent application data. Its initialized settings and newly recreated process were checked; broker credentials and static AWS credentials were absent. The production host checkout remains `b061876` to preserve unrelated host changes; the running image is pinned in the isolated compose bootstrap.
- EC2 role has zero attached managed policies and six reviewed inline policies: exact web parameter reads, SSM agent channels without parameter access, exactly three unqualified reader invokes, web/frontend image pulls, explicit role-assumption deny, and the existing snapshot S3 write permissions. The old wildcard `/fintracker/*` policy and broad SSM/ECR managed policies were removed. Applied-policy simulation passed 503 resource-specific checks; Access Analyzer returned no findings. Resource policy, trust, and metadata reviews are recorded in the verification artifacts.
- Old IOL/PPI parameters and their historical versions under `/fintracker/prod` were deleted only after the replacement and financial equivalence checks passed. Worker `SecureString` parameters remain. The old host `.env` was replaced from scratch with nonsecret bootstrap settings without reading it; no credential rotation was performed. No evidence of theft was established, and existing keys remain confined to the worker store.
- Authenticated public HTTPS Refresh succeeded after cleanup and backend recreation. A scheduled systemd valuation also completed successfully. Both preserved all source position/quantity fingerprints, existing manual holdings, valuation history and price history; broker valuation fields matched the pre-cutover baseline. A same-value manual update/readback passed and preserved original file bytes. No financial amounts or hashes are stored in these public artifacts.
- The valuation timer is active and enabled; next run is 2026-10-25 16:00 UTC. Existing three unpriced Binance balances remained unpriced as in the baseline; they caused no new job errors.
- Binance code is wrapped by a fail-closed response/log guard. Read-only API capability checks confirmed trading, withdrawals, futures and transfer permissions are disabled. Existing legacy balance results matched the replacement. No keys were rotated.
- Backend suite: 113 passed, with nine existing deprecation warnings. CI run [40118168893](https://github.com/julianlizarraga15/fintracker/actions/runs/40118168893) passed backend, broker package and browser checks. PR 21 remains open for review. Do not enable the web CI/CD workflow as part of routine release steps.

## Durable evidence

- [Cutover summary](docs/verification/20261024-broker-cutover.json)
- [Applied EC2 policy](docs/verification/20261024-web-runtime-policy.json)
- [Applied policy simulations](docs/verification/20261024-web-iam-checks.json)
- [Worker image scan](docs/verification/20261024-broker-image.json)
- [Operational plan](docs/plans/20261024-broker-credential-isolation.md)
- [Deployment and rollback runbook](docs/broker-credential-isolation.md)
- Decision trail: `.audit/credential-isolation.tsv` (local checklist is ignored and contains no credentials)

## Guardrails and rollback

Keep web CI/CD disabled. For a worker rollback, deploy a reviewed CloudFormation change set using a verified safe reader version/image. The web uses unqualified function ARNs, so publishing alone does not route traffic. Keep the credential-free web configuration and least-privilege IAM in place; do not restore broker credentials to EC2. If a reader fails, preserve the last complete snapshot, pause only affected valuations if needed, and restore a safe reader release. Recreate the backend only with the explicit isolated compose file and clean bootstrap described in the runbook; do not use default Compose discovery, `down`, or orphan removal.

No further isolation work is blocked on AWS login. Rotation is a separate credential-owner decision; no rotation was required to complete this boundary.
