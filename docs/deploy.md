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
signs with its role; to smoke-test from a shell, use `awscurl` or
`curl --aws-sigv4`. The URL:
```
aws cloudformation describe-stacks --stack-name sector-in-brief-api-stg --query "Stacks[0].Outputs" --output table --profile thiya --region us-east-1
```

## Not in slice 1 (later slices)
Durable `/download/{job_id}` + request registry, default-on email receipt,
NDJSON telemetry + monthly rollup, prod stage, and the soak/cutover (ADR 0008).
