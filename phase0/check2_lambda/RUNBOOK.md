# Build step 0 — Check 2 probe runbook (operator-run)

Gates the **Lambda-first** host decision: does the wide-tail DuckDB join complete
inside a 10 GB Lambda, and at what in-region S3-write throughput? You run the
deploy/invoke (control-plane, under your identity); the slice itself is authored
here. No Docker needed — `sam build` zips the manylinux duckdb wheel (~58 MB,
well under Lambda's 250 MB limit). Commands are single-line; run in order.

## Deploy

```
cd phase0/check2_lambda
```
```
sam build
```
```
sam deploy --stack-name sector-in-brief-api-step0-stg --capabilities CAPABILITY_IAM --resolve-s3 --parameter-overrides Stage=stg --no-confirm-changeset --profile thiya --region us-east-1
```

This creates the **real** results bucket `sector-in-brief-api-results-stg`
(30-day lifecycle) and the probe Lambda `sector-in-brief-api-check2-stg`.

## Invoke — the wide tail (the actual test)

Run the heaviest query (wide projection, all years, all states) and print the
Lambda REPORT line (has **Max Memory Used** + **Duration**):
```
aws lambda invoke --function-name sector-in-brief-api-check2-stg --cli-binary-format raw-in-base64-out --payload '{}' --log-type Tail /tmp/out.json --query LogResult --output text --profile thiya --region us-east-1 | base64 -d
```
Then the handler's returned JSON (rows, bytes, copy throughput):
```
cat /tmp/out.json
```

Paste me both. What I'm looking for:
- **Did it succeed** (no `errorType`/OOM kill)?
- **Max Memory Used** vs the 10240 MB cap — the headroom (or lack of it).
- **copy_MB_s** to the real bucket — the deferred Check 1 (in-region S3-write rate).

If you want the comparison points too (optional), invoke with a payload of
`{"wide": false}` (narrow projection) or `{"state": "CA"}` (one state) — same
command, different `--payload`.

## Interpreting / next

- **Completes within 10 GB** → Lambda-first confirmed; proceed to the full rewrite
  (real handler replaces this probe in the SAME stack; bucket/IAM stay).
- **OOMs / times out** → the wide tail pivots to an async non-Lambda worker; the
  hybrid threshold moves. We record it and adjust before the rewrite.

## Do NOT tear down

Keep the stack — it owns the results bucket and seeds the build. Only the probe
function gets replaced by the real handler as the build proceeds. (If you ever
must abandon: `sam delete --stack-name sector-in-brief-api-step0-stg --profile
thiya --region us-east-1` — note the bucket must be emptied first.)
