# Deploy (staging) — sector-in-brief-api

Operator-run (control-plane stays under your identity). Stack: `sector-in-brief-api-stg`.

## One-time: retire the Phase-0 probe stack

Build step 0 created `sector-in-brief-api-step0-stg`, which owns the results
bucket. The real stack recreates that bucket, so delete the probe stack first (its
bucket only ever held transient probe output, all deleted):
```
sam delete --stack-name sector-in-brief-api-step0-stg --no-prompts --profile thiya --region us-east-1
```

## Build + deploy
```
cd /root/NCCS/sector-in-brief-api
```
```
sam build
```
```
sam deploy --stack-name sector-in-brief-api-stg --capabilities CAPABILITY_IAM --resolve-s3 --parameter-overrides Stage=stg --no-confirm-changeset --profile thiya --region us-east-1
```

Creates: results bucket `sector-in-brief-api-results-stg` (30-day lifecycle), the
`sector-in-brief-api-query-stg` Lambda (10 GB / 900s / 10 GB ephemeral), and its
Function URL (`AuthType: AWS_IAM`). The Function URL is in the stack outputs.

## Test — direct invoke (bypasses Function URL SigV4; easiest)
The handler accepts both Function-URL events and raw direct-invoke payloads, so
`events/event.json` (the request body) works as a direct payload:
```
aws lambda invoke --function-name sector-in-brief-api-query-stg --cli-binary-format raw-in-base64-out --cli-read-timeout 900 --payload file://events/event.json /tmp/out.json --profile thiya --region us-east-1
```
```
cat /tmp/out.json
```
Expect `200` with `row_count`, a `result` presigned URL, and a `data_dictionary`
presigned URL. Open the result URL to confirm the CSV; open the dictionary URL to
confirm the merged dictionary.

## Test — the Function URL itself (as the dashboard will call it)
`AuthType: AWS_IAM` means callers sign SigV4. From the server side the dashboard
signs as a dedicated IAM user (see *Dashboard caller credentials* below); to
smoke-test from a shell, use `awscurl` or `curl --aws-sigv4`. The URL:
```
aws cloudformation describe-stacks --stack-name sector-in-brief-api-stg --query "Stacks[0].Outputs" --output table --profile thiya --region us-east-1
```

## Dashboard caller credentials (how the dashboard signs POST /data)
`POST /data` is `AuthType: AWS_IAM`, so the caller — the `sector-in-brief` Shiny
server — must sign with an AWS identity allowed to invoke the query function.

shinyapps.io has **no instance role and no OIDC**, and the dashboard invokes the
function at **runtime** (when a user submits the form), not in CI — so a role is
not an option there. In particular, do **not** try to reuse the
`sector-in-brief-api-github-deploy` role: it is a GitHub-Actions OIDC role
(`sts:AssumeRoleWithWebIdentity`, trusts `token.actions.githubusercontent.com`
scoped to this repo) and can only be assumed inside a CI run.

Instead, create one dedicated, minimally-scoped **IAM user** with a long-lived
access key (account `672001523455`):
```
aws iam create-user --user-name sector-in-brief-dashboard-invoke --profile thiya
```
```
aws iam put-user-policy --user-name sector-in-brief-dashboard-invoke \
  --policy-name invoke-query \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":["arn:aws:lambda:us-east-1:672001523455:function:sector-in-brief-api-query-stg","arn:aws:lambda:us-east-1:672001523455:function:sector-in-brief-api-query-prod"]}]}' \
  --profile thiya
```
```
aws iam create-access-key --user-name sector-in-brief-dashboard-invoke --profile thiya
```
The policy grants only `lambda:InvokeFunction` on the query function ARNs (stg +
prod; prod doesn't exist yet, which is harmless). The `create-access-key` output's
`SecretAccessKey` is shown **once** — capture it.

Hand the key to the dashboard, never commit it: in the `sector-in-brief` repo set
GitHub Actions secrets `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`; that repo's
deploy workflows forward them to shinyapps.io as encrypted env vars (rsconnect
`envVars`), where `paws.compute` reads them from the standard credential chain.
Region is `us-east-1`.

Rotate without downtime: `create-access-key` (a second key) → update the GitHub
secret → redeploy → `delete-access-key` (the old one). This account is separate
from the institutional `nccsdata` account, so its 24h access-key rotation policy
(the reason the S3 data sync is anonymous) does not apply here.

## Not in slice 1 (later slices)
Durable `/download/{job_id}` + request registry, default-on email receipt,
NDJSON telemetry + monthly rollup, prod stage, and the soak/cutover (ADR 0008).
