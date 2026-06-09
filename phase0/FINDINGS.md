# Phase-0 findings — DuckDB path proven; host decision now data-bounded

ADR 0008 / 0026 vertical slice, run 2026-06-09 against live `s3://nccsdata` and
the legacy `nccs-dataexplorer-stg/results/` bucket (AWS profile `thiya`, account
`672001523455`, us-east-1). Artifacts: `phase0/duckdb_query.py`,
`phase0/measure_results.py`. Spike S3 output was cleaned up after measuring.

## Gate 1 — the DuckDB-on-parquet path works (correctness) ✅

One real dashboard-style query, end to end: **CORE 990 parquet ⋈ bmf-master-
geocoded parquet, joined on `EIN` at query time** (ADR 0016 — no pre-merged
table), filtered on the geocoded state, projected to 8 columns, **materialized
straight to S3** (ADR 0026 pattern B), returned as a presigned URL.

- `geo_state_abbr=CA`, 2019/990 → **32,751 rows**, 2.91 MB CSV. Path runs clean.
- **Join is sound (no fan-out):** `ein` is **unique** in bmf-geocoded (3,672,933
  rows, all distinct). Inner join preserves all 315,633 core filings except **9**
  (0.00%) whose EIN is absent from BMF. The 925 duplicate EINs in core are a
  core-side filing characteristic, not a join artifact (the legacy merged table
  carried them too).
- **No Athena parity check is meaningful:** the legacy API queried the now-retired
  pre-merged `CORE_FULL_V0_1` table; ADR 0016 dropped that artifact. Correctness
  is therefore verified by join semantics, not by output diff.

### Ground-truth corrections to the contracts/CLAUDE.md mental model
- **CORE is CSV-canonical, parquet present-but-not-contracted.** `core-990.yml`
  marks CSV canonical and parquet "not yet contract-canonical"; the parquet files
  *do* exist on S3 (`core_2019_990.parquet`, written by the producer's
  `R/09_parquet.R`) and DuckDB reads them fine. **The core parquet migration is a
  named prerequisite (ADR 0003 / core-990.yml open item #3)** before we depend on
  it in production. bmf-master-geocoded parquet *is* contract-canonical.
- **DuckDB's own `credential_chain` cannot resolve this account's SSO assumed-role
  session.** We resolve credentials via boto3 and hand DuckDB an explicit S3
  secret — the same shape the eventual instance-role → boto3 → DuckDB flow uses.

## Gate 2 — result-size distribution (decides the host) ✅

From **2,539 real production result CSVs** (network-independent; the binding fact):

| stat | size | | threshold | % of queries over |
|------|------|-|-----------|-------------------|
| p50  | 0.1 MB   | | > 6 MB (API GW sync cap)     | **38.5%** |
| p75  | 117 MB   | | > 100 MB (0008 sync line)    | **25.6%** |
| p90  | 2.7 GB   | | > 512 MB (Lambda /tmp dflt)  | 17.4% |
| p95  | 11.7 GB  | | > 10 GB (Lambda max mem/tmp) | **5.6%** |
| p99  | 30.7 GB  | | | |
| max  | **51.2 GB** | | mean | 1.6 GB *(lies — bimodal)* |

The distribution is violently bimodal: **the median query is trivial (0.1 MB)**,
but the tail is enormous. The mean (1.6 GB) describes no actual query.

**What this settles, regardless of host:**
- **Pattern B is mandatory, not a preference.** 38.5% of results exceed even the
  6 MB API Gateway response cap; the bytes can never flow back through the API
  process. ADR 0026's "always materialize → S3 → presigned URL" is confirmed by
  data.
- **A pure synchronous Lambda is non-viable.** 5.6% of real results exceed
  Lambda's hard 10 GB memory/`/tmp` ceiling outright; 25.6% exceed the 100 MB
  async line.

## What the spike could NOT decide — and the next measurement

Latency was measured **from a local WSL machine**, so every httpfs read and S3
write crossed the public internet, not the in-region AWS network. Treat these as
a badly biased **lower bound**, not the in-region truth:
- 3.75M-row all-years 990 ⋈ bmf join **counted in 5.8s** (DuckDB column/predicate
  pushdown is excellent), but **materializing 0.32 GB took 219s (~1.5 MB/s)** —
  dominated by local→S3 egress and full remote parquet reads with no cache.

So the Lambda-vs-App-Runner line for the **materialization worker** is not yet
nailed; it needs an **in-region** rerun. But the size distribution already bounds
it hard.

> The misleading-mean result here is generalized as reusable technical-writing
> material in [`docs/fat-tails-and-the-misleading-mean.md`](../docs/fat-tails-and-the-misleading-mean.md).

## Recommendation (for go/no-go)

1. **Adopt pattern B uniformly** for the form path — **DECIDED 2026-06-09**
   (confirmed by data: 38.5% of results exceed the 6 MB inline cap).
2. **Host: App Runner for materialization** (or an always-on/async non-Lambda
   worker), because the heavy tail (5.6% > 10 GB, max 51 GB) exceeds Lambda's hard
   ceilings and any 15-min wall. A **hybrid** stays open — Lambda/light sync for
   the 0.1 MB median common case, App Runner/async for the tail — and App Runner's
   persistent process buys a local DuckDB cache to amortize httpfs reads.
3. **Confirm with one in-region measurement** before committing IaC: run
   `phase0/duckdb_query.py` on a p90-ish (multi-GB) query from inside us-east-1
   (a throwaway EC2/App Runner/Lambda in-region) and record true throughput.
4. **Track the core-parquet prerequisite** (ADR 0003) — production reads should
   not depend on the present-but-uncontracted core parquet until it's canonical.

## Gate question
Go on **App Runner (or async worker) for materialization, pattern B uniformly**,
pending the one in-region latency confirmation? On "go" the next step is the real
rewrite (handler + `template.yaml` results bucket/lifecycle + `/download/{job_id}`
+ registry + SES receipt + NDJSON telemetry) and deleting the Athena scratch.
