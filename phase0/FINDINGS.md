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

## Gate 3 — in-region latency (measured 2026-06-09, EC2 c5.2xlarge, us-east-1) ✅

The WSL spike measured latency **from outside AWS**, so every httpfs read crossed
the public internet — a badly biased lower bound. The in-region rerun (operator-run
EC2 to keep control-plane calls off the assistant session) settles it. Output
written to local `/tmp` to isolate the read-from-S3 + join cost (the box's
`ec2-s3FullAccess` role can read `nccsdata` but is denied PutObject on the results
bucket; the in-region S3 upload of the result is fast and not the bottleneck):

| tier | rows | result | count | copy | throughput |
|------|------|--------|-------|------|------------|
| small (one state-year) | 32,751 | 2.9 MB | 1.7s | 2.4s | ~4s total wall (overhead-bound) |
| medium (all years × all states) | 3,751,511 | 331 MB | 1.4s | 3.2s | **104 MB/s** |

**The same all-years join that took 219s / 1.5 MB/s from WSL took 3.2s / 104 MB/s
in-region — a ~70× speedup.** The WSL latency was almost entirely egress; it is
*not* a host signal.

### This reframes the host decision — throughput is no longer the binding limit

At ~100 MB/s, Lambda's **15-min wall is ~90 GB of headroom** — larger than the
51 GB observed max. So wall-time no longer rules Lambda out, and with DuckDB
streaming `COPY` straight to S3 (no full-result buffering) the 10 GB `/tmp` ceiling
doesn't bind either. The remaining Lambda constraint is **join memory** (10 GB cap)
on the widest/largest queries — a narrower question than "can it finish in time."

- p50 (0.1 MB) / p75 (117 MB): materialize in **well under a second to ~2s** — trivially Lambda.
- ~10 GB (p95): **~100–200s** end-to-end — inside Lambda's 900s wall, *if* the join fits in 10 GB memory.
- 30–51 GB (p99–max): **~300–1000s** — approaches/exceeds the wall, and wide joins may exceed 10 GB memory. These few still want an async/long-running worker.

### Still open before committing IaC (narrowed)
1. **In-region S3-write throughput** — we measured local write; confirm the result
   upload rate (expected fast, but close the loop).
2. **Peak join memory on the wide/`large` tier** vs Lambda's 10 GB cap — run the
   `c.*` wide projection and watch RSS.
3. **Confirm DuckDB streams `COPY` to S3** without buffering the whole result.

> The misleading-mean result here is generalized as reusable technical-writing
> material in [`docs/fat-tails-and-the-misleading-mean.md`](../docs/fat-tails-and-the-misleading-mean.md).

## Recommendation (for go/no-go)

1. **Adopt pattern B uniformly** for the form path — **DECIDED 2026-06-09**
   (confirmed by data: 38.5% of results exceed the 6 MB inline cap).
2. **Host: Lambda-first hybrid** (revised after the in-region measurement, which
   reversed the earlier App-Runner lean). In-region throughput (~100 MB/s) puts
   the p50–p95 of the distribution — everything up to ~10 GB — comfortably inside
   Lambda's 15-min wall, so **Lambda handles the overwhelming majority of
   materializations**. Route only the rare p99+ giant (30–51 GB) and any
   memory-heavy wide join to an **async non-Lambda worker** (Fargate/App
   Runner/Batch), surfaced through ADR 0026's durable `/download/{job_id}` so the
   caller just polls. Decide Lambda-only-with-async-tail vs always-async after
   the two narrowed checks above (S3-write rate, wide-join memory).
3. **Track the core-parquet prerequisite** (ADR 0003) — production reads should
   not depend on the present-but-uncontracted core parquet until it's canonical.

## Gate question
In-region latency is in: **pattern B uniformly + a Lambda-first hybrid**
(Lambda materializes p50–p95; async worker only for the p99+ giant). Two narrowed
checks remain (in-region S3-write rate; wide-join peak memory vs Lambda's 10 GB).
Go on starting the real rewrite (handler + `template.yaml` results
bucket/lifecycle + `/download/{job_id}` + registry + SES receipt + NDJSON
telemetry), running those two checks alongside the build?
