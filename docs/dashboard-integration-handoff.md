# Handoff prompt — paste into a Claude Code session opened in the `sector-in-brief` repo

You are working in the **`sector-in-brief`** repo — the Shiny dashboard. The
modernized data API, **`sector-in-brief-api`** (sibling repo at
`../sector-in-brief-api`), is now built and deployed to staging (DuckDB-on-parquet,
per ADR 0008/0026). Your job is **prong 1 of ADR 0026 §6: rework the Data-Download
form** to call the new API instead of the legacy Athena endpoint. Do not re-derive
the API design — it's fixed; build the form against its contract.

## Read first
- `../sector-in-brief-api/openapi.yaml` — the interface contract (request/response).
- `../sector-in-brief-api/docs/deploy.md`, `phase0/FINDINGS.md` — what was built/why.
- This repo: the current download form (per project notes, **`R/query_builder_download.R`**)
  and the viz panels (which read S3 directly — leave them alone, see below).
- ADR 0026 (esp. §6) and ADR 0011 in `../nccs-contracts/decisions/`.

## The API surface you integrate with

Two endpoints, **different auth** (this is the key design point):

- **`POST /data`** — runs the export. Function URL `AuthType: AWS_IAM`, so the
  **Shiny server** authenticates (server-to-server; never the browser). Two ways:
  - **Recommended — direct Lambda invoke** via the `paws` R package:
    `paws::lambda()$invoke(FunctionName="sector-in-brief-api-query-stg", Payload=<json>)`.
    The SDK signs; no URL signing needed. The handler accepts the raw request JSON
    as the invoke payload (it handles both Function-URL and direct-invoke events).
  - Alternative — SigV4-sign a POST to the Function URL
    `https://mz66675k3bkjp5n7zcuvoi5nry0cwysj.lambda-url.us-east-1.on.aws/`
    (service `lambda`) with `aws.signature`/`httr2`.
- **`GET /download/{job_id}`** — the durable link. **Public** Function URL (no
  signing): `https://w5tws2ws3racy4des7afbtpdya0gpzln.lambda-url.us-east-1.on.aws/download/{job_id}`.
  This is what the API emails the user; you also show it in-browser. Append
  `?kind=dictionary` for the data dictionary.

(Re-fetch URLs if the stack is redeployed:
`aws cloudformation describe-stacks --stack-name sector-in-brief-api-stg --query "Stacks[0].Outputs" --profile thiya --region us-east-1`.)

### Request (POST /data)
```json
{
  "tax_years": [2019, 2020],
  "forms": ["990", "990ez", "990pf", "990combined"],
  "columns": ["ein", "org_name_display", "geo_state_abbr", "nteev2", "nteev2_subsector", "total_revenue"],
  "filters": { "geo_state_abbr": ["CA", "NY"], "nteev2_org_type": ["..."] },
  "format": "csv",
  "email": "user@example.org"
}
```
- **NEW parquet column names** — there is **no backward-compat mapping** to the
  legacy pre-merged table's names (`CENSUS_STATE_ABBR`, `TAX_YEAR`, `Size`, …). The
  data engineering was overhauled; the legacy API reads stale datasets. Send the
  new names; the API validates them against the live schema and 400s on unknowns.
- `filters` values are `WHERE col IN (...)`. `email` triggers a default-on receipt.

### Response
`{ job_id, row_count, result:{format,bytes,url,expires_in_seconds},
data_dictionary:{url,columns}, download_path, download_url, dictionary_download_url,
email:{to,status} }`. The `url`s are short-lived presigned S3 links; `download_url`
is the durable public one.

## What the form must do (ADR 0026 §6)
1. **Filter inputs that map 1:1 to the request schema** (new column names).
2. **Default column selection** — *the dashboard owns this.* Pre-select the usually-
   wanted set (`org_name_display`, `nteev2`, `nteev2_subsector`, …), user-deselectable.
   The API force-includes only `ein` (the key); everything else is your choice.
3. **Size estimate before committing** — call `POST /data` with `"estimate": true`
   (returns `{row_count, columns, estimated_bytes}` fast, no materialize/email).
   Show "~N rows / ~M MB" and warn before a large export. *(API computes the
   estimate; you own the warn/confirm UX.)*
4. **Deliver via link, not bytes** — the result **never flows through Shiny**. Show
   the `result.url` (or durable `download_url`) for the browser to pull from S3
   directly, plus an explicit "we've also emailed this to you."
5. **Progress state** for the rare slow export; **CSV primary**, parquet optional.

## Filter mapping — and the gaps to coordinate

| Legacy form filter | New API column | Status |
|---|---|---|
| `CENSUS_STATE_ABBR` | `geo_state_abbr` | ✅ supported |
| org type | `nteev2_org_type` | ✅ supported |
| `SUBSECTOR` | `nteev2_subsector` | ✅ supported |
| `TAX_YEAR` | `tax_years` (partition selector, not a filter) | ✅ supported |
| `CENSUS_COUNTY` | `geo_county` is the **raw geocoder label** only | ⚠️ **API gap** |
| `CENSUS_CBSA` | `geo_metro_area` (name) only | ⚠️ **API gap** |
| `CENSUS_REGION` | none | ⚠️ **API gap** |
| `Size` (asset bucket) | `total_assets_eoy` is numeric; API does `IN`, not ranges | ⚠️ **API gap** |

**Coordinate these back to the API maintainer before relying on them:** reliable
county/CBSA/region filtering needs the API to join the published crosswalks
(county-fips, cbsa, CT planning-region — ADR 0021/0023), and asset-size needs
either a derived size column or range-filter support. For a first cut, ship the
✅ filters (state / org type / subsector / years) and stage the ⚠️ ones pending API
enhancements — don't silently map them to unreliable raw columns.

## Don't break the viz panels
The dashboard is a **hybrid consumer** (ADR 0011): visualization panels read S3
**directly** and must keep doing so. Only the **download section** changes to call
this API. Don't reroute the viz reads.

## Known bug to fix at cutover
The download form has a wiring bug — `filters[["ASSET_SIZE"]] <- inputs$asset_select`
should be `inputs$size_select`. Fix it as part of this rework (verify against the
current `R/query_builder_download.R`).

## Cutover (ADR 0008 — don't flip the live pointer yet)
Build + test against the **staging** API. Cutover is sequenced: shadow/soak against
real queries, switch the UI pointer, monitor a week, then 90-day sunset of the old
API. The legacy endpoint is a hardcoded `/stg/` URL (ADR 0011 residual #1) — make
the new endpoint config-driven so the pointer switch is a config change.

## How to proceed
Follow this repo's conventions. Build against `../sector-in-brief-api/openapi.yaml`;
keep the new column names; surface the filter gaps above rather than papering over
them. Summarize your plan (form fields, default columns, estimate UX, the paws-vs-
SigV4 call) before implementing.
