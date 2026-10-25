# Deploy isolated broker readers

**Production cutover completed 2026-10-24.** This document is now an operational and rollback reference, not an instruction to repeat the initial migration. Live evidence is in [PROJECT_STATUS.md](../PROJECT_STATUS.md) and [the sanitized cutover summary](verification/20261024-broker-cutover.json). Keep web CI/CD disabled. Do not read `.env` contents or restore broker credentials to the web host.

For any later backend recreation, use only the tracked `docker-compose.isolated.yml` and host `.isolation-bootstrap`, with explicit `--env-file`, project and file arguments. The bootstrap contains nonsecret settings including `SSM_ENV_PATH=/fintracker/web`, both AWS region variables, `PYTHON_DOTENV_DISABLED=1`, and the reviewed backend image tag. The regular Compose file is not used for this service. Never run `docker compose down`, remove orphans, or recreate the unrelated frontend, proxy, or watering services as part of backend rollback.


The procedures below document the initial migration and are retained for audit context. Production is already cut over; consult the verified operation and rollback sections at the end before making any change. These procedures alone do not establish current live state.

Run AWS commands with your deployment identity in `us-east-1`, never with `fintracker-ec2-role`. Keep that role unable to deploy or inspect IAM. The existing Binance reader remains in `sa-east-1`.

## 1. Prepare a controlled release

1. Keep this change on a feature branch until you are ready for the web cutover. A push to `main` runs the existing `ci-cd.yml` and deploys the web app automatically.
2. Before merging, disable the existing `ci-cd` workflow in GitHub Actions. Merge only after reviewing the complete change. Leave automatic deployment disabled until the web configuration and IAM are ready. GitHub requires a `workflow_dispatch` definition on the default branch before its manual trigger is available.
3. Run the backend tests and synthetic worker tests. Review responses and captured success and failure logs for authentication data, including invocation `LogType=Tail`. Do not enable SDK or HTTP debug logging in production.
4. Confirm account-specific broker location and outbound IP restrictions. The template supplies no VPC, NAT Gateway, Function URL, event source, or public invoke permission. A broker that requires a fixed outbound IP needs a separate network design.
5. Inventory the current source enablement, nonsecret application `ACCOUNT_ID`, PPI account selection, snapshot location, and daily timer. Keep the last working snapshots. Avoid an in-flight valuation during a web restart.

The worker image uses the Lambda Python 3.12 base, copies only `backend`, and supports `linux/amd64`. Build from the clean GitHub checkout. Never add `.env`, secret files, or credentials as build arguments. The deployed digest identifies the exact built artifact. The worker requirements pin `ppi-client==1.2.4`, including the public SDK bridge used to enforce per-request timeouts. Test every new digest. The root `.dockerignore` excludes environment files, local data, and credentials from the build context.

## 2. Bootstrap ECR, then build the image

Use the ECR console to create private repository `fintracker-broker-sources` in `us-east-1`, with immutable tags and scan on push. The repository must exist before running the build workflow or creating the function stack. CloudShell does not need Docker for this procedure.

Give the separate GitHub build identity ECR authorization and push access to this repository. Follow the existing `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` GitHub secret setup. Do not put broker keys in GitHub. Review that identity separately from the EC2 role. The EC2 role must have no write access to this repository or the repository policy.

In the ECR repository permissions, grant Lambda image retrieval for these functions. Replace `AWS_ACCOUNT_ID` in this policy with your AWS account number.

```json
{
	"Version": "2012-10-17",
	"Statement": [{
		"Sid": "BrokerLambdaImageRead",
		"Effect": "Allow",
		"Principal": {"Service": "lambda.amazonaws.com"},
		"Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
		"Condition": {
			"StringEquals": {"aws:SourceAccount": "AWS_ACCOUNT_ID"},
			"ArnLike": {"aws:SourceArn": [
				"arn:aws:lambda:us-east-1:AWS_ACCOUNT_ID:function:fintracker-fetch-iol-data",
				"arn:aws:lambda:us-east-1:AWS_ACCOUNT_ID:function:fintracker-fetch-ppi-data"
			]}
		}
	}]
}
```

Run **Build broker Lambda image** manually from the reviewed commit. The workflow upgrades OS packages from the pinned AL2023 release, builds, records RPM versions, runs the network-disabled image smoke check, and pushes only after that check passes. It then waits for the ECR scan of that exact digest, saves the scan artifact and fails on HIGH or CRITICAL findings, absent findings metadata, or a digest mismatch. A pushed image from a failed scan job is not approved for deployment. It does not deploy the functions. Copy the `Image URI` from its job summary. It must end with `@sha256:` and a 64-character digest. Retain that image for rollback and keep it out of any ECR lifecycle rule that expires active or previous working digests. Lambda requires a single-architecture image. See [Deploy Python Lambda functions with container images](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html).

## 3. Create worker credentials securely

In the Systems Manager Parameter Store console in `us-east-1`, create these exact names as `SecureString` values using the broker portal or your existing secure secret manager:

- `/fintracker/workers/iol/IOL_USERNAME`
- `/fintracker/workers/iol/IOL_PASSWORD`
- `/fintracker/workers/ppi/PPI_PUBLIC_API_KEY`
- `/fintracker/workers/ppi/PPI_PRIVATE_API_KEY`

Enter values directly into the console fields. Do not put them in chat, CLI arguments, shell history, command output, logs, templates, or this repository. Do not inspect any `.env` file. If you cannot source a credential securely, use the broker's credential replacement flow. Keep the current production route working until its replacement is verified.

Use the default AWS-managed SSM key unless your requirements call for a customer-managed key. Default-key encryption depends on correct SSM authorization to separate readers. It is not an independent per-role KMS barrier. For a customer-managed key, set the matching `IolKmsKeyArn` or `PpiKmsKeyArn` stack parameter and review the key policy. The role's decrypt grant is restricted by the SSM service, AWS account, and exact `PARAMETER_ARN` encryption contexts. See [Parameter Store KMS encryption](https://docs.aws.amazon.com/systems-manager/latest/userguide/secure-string-parameter-kms-encryption.html).

The template contains parameter names, never credential values. Each invocation reads its worker's two exact parameters with `ssm:GetParameters`. Workers cannot read the other broker's parameters, parameter history, or recursive paths through these policies. Review other grants before relying on that statement for the deployed roles.

## 4. Review and deploy the worker stack

Set these shell variables to nonsecret metadata. `BROKER_IMAGE_URI` is the workflow's digest URI. `APP_ACCOUNT_ID` is the existing application identifier, not your AWS account number or your PPI account number.

```bash
	export BROKER_IMAGE_URI='PASTE_REVIEWED_ECR_DIGEST_URI'
	export APP_ACCOUNT_ID='EXISTING_APPLICATION_ACCOUNT_ID'
	aws cloudformation validate-template --region us-east-1 \
	  --template-body file://infra/broker-sources.yaml
	aws cloudformation deploy --region us-east-1 \
	  --stack-name fintracker-broker-sources \
	  --template-file infra/broker-sources.yaml \
	  --capabilities CAPABILITY_IAM \
	  --parameter-overrides "ImageUri=$BROKER_IMAGE_URI" "AccountId=$APP_ACCOUNT_ID" \
	  --no-execute-changeset
```

If needed, add the fixed nonsecret `PpiAccountNumber` parameter and the optional KMS ARN parameters. Do not add broker credential values. In CloudFormation, review the generated change set and execute it when ready. Existing functions or log groups with these names require deliberate import or reconciliation before stack creation. Do not delete them to make creation succeed.

The stack creates separate generated role names, `fintracker-fetch-iol-data`, and `fintracker-fetch-ppi-data`. Each function has 1 GiB memory, reserved concurrency 1, and a default 900-second timeout. IOL needs time for both portfolio markets and authenticated quote calls. The web adapter must allow more than the configured function timeout and avoid automatic SDK retries. Measure actual runtimes before lowering the timeout. Confirm the account has enough unreserved Lambda concurrency to reserve two slots.

CloudWatch retains logs for 14 days. The stack retains log groups on deletion. Worker roles can write only their own log streams. They cannot create arbitrary log groups. This does not sanitize logs by itself. Callers can receive execution log tails through Lambda Invoke, so handler and SDK output must remain sanitized. See [Lambda Invoke](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html).

## 5. Verify workers before changing the web route

1. Use the Lambda console **Test** with exactly `{}` for each function. The account, credential names, and operation are fixed by deployment. The caller must not supply account overrides, endpoints, credentials, or operations.
2. Privately compare each response with the current source's positions and prices. Check IOL domestic and US holdings, market fallback, and authenticated quotes. Check PPI cash, currency, instrument type, and settlement. Keep portfolio data out of shared logs and artifacts.
3. Require a valid schema, source status `ok`, expected counts, and acceptable prices. An HTTP success, a top-level valuation success, or an empty list alone does not prove source success. An empty account needs explicit confirmation.
4. Check execution logs and invocation tails for authentication output. Test failure sanitization with synthetic credentials in the test suite or a separate test function. Do not deliberately break the production credentials to test errors.
5. Record the tested image digest, function revision, counts, and timings. Publish a working Lambda version before the next update if you want a versioned rollback target.

These are staged source checks while the current web app still runs. Do not remove old credentials or narrow production SSM access until these checks pass.

## 6. Prepare web configuration and review effective IAM

Create the web's needed configuration under `/fintracker/web` using the console. Keep web authentication secrets as `SecureString`. Copy only settings allowed by `backend/core/settings.py`; never copy a whole old parameter tree. Resolve whether `TEST_SECRET` is used before removing it.

Set `SSM_ENV_PATH=/fintracker/web` in the approved container configuration. The new web loader uses `ssm:GetParameters` on an explicit allowlist. Grant that action on every exact allowlisted ARN, including names not currently created, because the loader requests all names in batches. Do not grant `GetParametersByPath`, `/fintracker/*`, `/fintracker`, or a wildcard parent. Include `ACCOUNT_EMAIL` so that the web calculates the same `ACCOUNT_ID` used by the workers.

Generate a reviewable web configuration statement from the code's allowlist. First set `AWS_ACCOUNT_ID` to your nonsecret AWS account number. This reads source code without importing the application or retrieving parameter values.

```bash
	python3 - <<'PY'
import ast
import json
import os
from pathlib import Path

module = ast.parse(Path("backend/core/settings.py").read_text())
names = next(ast.literal_eval(node.value) for node in module.body
             if isinstance(node, ast.Assign)
             and any(isinstance(target, ast.Name) and target.id == "WEB_SSM_NAMES"
                     for target in node.targets))
account = os.environ["AWS_ACCOUNT_ID"]
print(json.dumps({"Version": "2012-10-17", "Statement": [{
    "Effect": "Allow", "Action": "ssm:GetParameters",
    "Resource": [f"arn:aws:ssm:us-east-1:{account}:parameter/fintracker/web/{name}"
                 for name in names],
}]}, indent=2))
PY
```

Configure `IOL_SOURCE_LAMBDA=fintracker-fetch-iol-data`, `PPI_SOURCE_LAMBDA=fintracker-fetch-ppi-data`, and `BROKER_LAMBDA_REGION=us-east-1`. Preserve the existing `BINANCE_BALANCE_LAMBDA` name and its `sa-east-1` region. Explicitly set `ENABLE_IOL=1` if IOL is enabled today. Set `ENABLE_PPI` and `ENABLE_BINANCE` to their intended existing states. Preserve public wallet addresses, storage settings, authentication settings, local manual holdings, job history, and snapshot paths.

The stack output `WebInvokePolicyForReview` is an additive example for exactly the three readers. Review and merge those statements with required web permissions. Do not replace the entire EC2 policy with that output. Adding this policy does not remove dangerous permissions from existing policies.

With your administrative identity, inspect every attached and inline policy for `fintracker-ec2-role`, its boundary, applicable SCPs, resource policies, KMS policies, and alternate credentials. Review the existing Binance role and code too. EC2's inability to list IAM policies is expected and is not evidence of complete isolation.

Remove or restrict all paths that let the web identity acquire broker secrets or change their readers. The review must cover:

- `ssm:GetParameter`, `ssm:GetParameters`, `ssm:GetParameterHistory`, and `ssm:GetParametersByPath` on current, old, and historical broker locations. Inspect grants on ancestor paths, including `/`, `/fintracker`, `/fintracker/prod`, and `/fintracker/workers`.
- `lambda:GetFunction`, `lambda:GetFunctionConfiguration`, worker update and permission APIs, Function URLs, layers, aliases, destinations, and version management. Keep exact `lambda:InvokeFunction` grants only for the approved readers.
- Worker log access, ECR image and repository-policy writes, image deletion, CloudFormation updates, deployment buckets, GitHub deployment credentials, SSM Run Command or automation routes, `iam:PassRole`, IAM policy edits, and `sts:AssumeRole` into stronger roles.
- Cross-account or resource-based grants and the worker role trust policies. The web must not assume either worker role.

Use IAM policy simulation and Access Analyzer under your administrative identity. Check each worker's own parameter reads are allowed and cross-broker reads are denied. Check the web's exact invokes and required web parameters are allowed while the prohibited actions above are denied. Include context keys where policies require them. Simulator results do not cover every resource policy, service behavior, or alternate credential path. Use the actual resource type for each simulation. Test IAM and STS actions against execution role ARNs, ECR actions against repository ARNs, CloudFormation actions against stack ARNs, and SSM Run Command against the instance and document ARNs. A denial for one of these actions against a Lambda ARN provides no useful evidence. Repeat simulations against the applied role after narrowing it. Record unresolved routes and stop cutover if they could expose credentials.

Do not probe a live broker parameter with a retrieval API merely to expect `AccessDenied`. If authorization is wrong, that API retrieves the secret even with a CLI output filter. Use metadata APIs for inventory and administrative policy evaluation for retrieval authorization. Denying a leaf parameter alone does not stop access through an allowed ancestor `GetParametersByPath` call. See [Restricting access to parameters and paths](https://docs.aws.amazon.com/systems-manager/latest/userguide/ps-restrict-parameter-access.html).

## 7. Cut over, clean up, and recreate web processes

1. After worker verification and IAM review, prepare the reviewed backend image and clean startup configuration. Pause the daily timer, drain active valuations, and stop the old backend before replacing its bootstrap configuration. The new application refuses to start if those variables remain. Keep all required web settings in `/fintracker/web` before replacing the old bootstrap configuration. Do not inspect `.env`; your secret-management procedure can replace it from scratch with only `SSM_ENV_PATH=/fintracker/web` and `AWS_REGION=us-east-1` once all other needed settings are persisted securely. Preserve local data and prevent manual refresh while the backend is stopped. Use a backend-only Compose recreation in step 2. A restart does not reload Compose's `env_file` values.
2. With the backend stopped and before starting its replacement, apply the reviewed web IAM restrictions from section 6. Verify the final applied role and preserve required application and SSM agent permissions. Keep legacy broker parameters temporarily inaccessible until equivalence passes. Recreate only the backend using the reviewed image, clean bootstrap and existing data mount. Keep web CI/CD disabled.
3. Run authenticated Refresh. Require successful individual IOL, PPI, and Binance reads where enabled. Compare source counts and valuation outputs privately. Confirm job history, price history, manual holdings edits, and the dashboard still work.
4. Using your console identity, remove old IOL and PPI copies from `/fintracker/prod` after equivalence passes. Removing current values alone is insufficient if historical values remain readable. Remove historical access or the obsolete parameters entirely. The previous Binance duplicate deletion is already complete and must not be repeated.
5. Confirm that old host configuration, backups, and deployment material no longer contain accessible broker copies. Do not read `.env` or print values. Rotate credentials if required by your exposure assessment, then verify the worker uses the replacement credentials.
6. Recreate the backend container and any long-lived job processes so old values cannot remain in memory. A plain `docker exec` environment check only examines a new process. A container restart alone can preserve its old startup environment. Check the application's initialized settings and its startup rejection checks using presence booleans only. Never dump `settings`, `os.environ`, `docker inspect`, `/proc/*/environ`, or raw Lambda configuration.
7. Re-enable the existing daily timer after Refresh passes and check a scheduled valuation through the existing systemd path. Confirm successful individual sources and persisted snapshots, job history, and prices. A startup HTTP 200 alone proves none of those behaviors.

After recreation, run this configuration probe inside the backend container:

```bash
	docker exec fintracker-backend python /app/scripts/check_broker_web_config.py
```

The probe disables dotenv loading, initializes only approved web settings, and prints booleans and source enablement. It does not inspect the already-running server's memory. Combine its result with the recreated container identity, the reviewed startup guard, and successful API startup. Do not treat this subprocess result alone as proof that an old process was cleared.

Save an audit note containing only booleans for forbidden credential presence, source enablement, expected SSM path, tested function identifiers, IAM review status, source counts, and timings. Include evidence from the actual app initialization and recreated process. If the inspection only covered a fresh subprocess, record that limitation and do not mark memory cleanup verified. Keep web deployment automation disabled unless the user explicitly requests enabling it.

## Roll back without returning credentials to the web

Keep the last working image digest and function configuration. To reverse a bad worker release, restore that digest in the reviewed stack change set. If using a published version, review its nonsecret configuration and credential references before promoting it. The web's unqualified function ARN invokes `$LATEST`; merely publishing a version does not change that route.

After IAM removal, do not deploy a web version that requires direct broker credentials. Keep the prior good snapshots and temporarily disable only the affected source if needed. Do not run a partial valuation that overwrites a good snapshot without reviewing the established missing-source behavior. Restore worker operation, then rerun Refresh and the scheduled path.

A template or this historical procedure alone cannot establish current isolation. The completed live review and cutover evidence are linked at the top; recheck current state before future production mutations.


## Verified production operation

The deployed EC2 role has zero attached managed policies and six reviewed inline policies; see [the exact applied policy](verification/20261024-web-runtime-policy.json) and [simulation summary](verification/20261024-web-iam-checks.json). Lambda quota is 1000 with one reserved slot per IOL/PPI reader. The backend uses `/fintracker/web`, disables dotenv, and has no broker credential in initialized settings. Old IOL/PPI parameter copies and history were deleted after successful equivalence checks; worker parameter copies remain.

A backend recreation must use a bounded operation such as:

```bash
cd /home/ec2-user/fintracker
docker-compose --env-file /home/ec2-user/fintracker/.isolation-bootstrap \
  -p fintracker -f /home/ec2-user/fintracker/docker-compose.isolated.yml \
  up -d --no-deps --force-recreate backend
```

The bootstrap is nonsecret, mode `0600`, and must contain the reviewed image reference plus `/fintracker/web`, region and dotenv settings. Do not let Compose discover the legacy `.env`. Before manual backend recreation, pause and drain scheduled valuations; afterward verify the new container identity and image, initialized settings, authenticated public Refresh, then restore the timer. If nginx has cached the previous backend IP, run `nginx -t` and reload the existing proxy after the backend replacement. Do not recreate other containers.

Rollback only to the reviewed worker image or safe published worker versions recorded in the cutover artifact. The backend invokes unqualified reader ARNs; publishing a version alone does not switch its route. Keep last complete snapshots and never put legacy broker credentials back into EC2. ECR basic scan coverage is limited to its documented scan scope and does not prove Python dependency coverage.
