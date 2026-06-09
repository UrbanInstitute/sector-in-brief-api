"""
Phase-0 vertical slice (ADR 0008 / 0026): prove the DuckDB-on-parquet path.

One real dashboard-style Data-Download query, end to end:
  contracted CORE 990 parquet  +  bmf-master-geocoded parquet   (read-only)
    -> DuckDB join on EIN at query time            (ADR 0016: no pre-merged table)
    -> filter (geocoded state) + column projection
    -> materialize result to S3                    (ADR 0026 pattern B)
    -> hand back a presigned URL

This is a SPIKE, not the production handler. It deliberately does NOT build the
durable /download/{job_id} endpoint, the request registry, email receipt, or the
real results bucket (those are post-gate). Credentials come from boto3 (the same
chain the eventual Lambda/App Runner instance role will use) and are handed to
DuckDB as an explicit S3 secret, because DuckDB's own credential_chain provider
cannot resolve the SSO assumed-role session this account uses.

Run:  AWS_PROFILE=thiya python phase0/duckdb_query.py [STATE] [TAX_YEAR]
"""
import sys, time, json, boto3, duckdb

# Read-only contracted inputs (verified on S3 2026-06-09). NB: CORE parquet is
# present but NOT yet contract-canonical (core-990.yml still marks CSV canonical;
# parquet via R/09_parquet.R). BMF-geocoded parquet IS canonical.
NCCS = "s3://nccsdata"
STATE     = sys.argv[1] if len(sys.argv) > 1 else "CA"
TAX_YEAR  = sys.argv[2] if len(sys.argv) > 2 else "2019"
CORE = f"{NCCS}/processed/core/{TAX_YEAR}/990/core_{TAX_YEAR}_990.parquet"
BMF  = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded.parquet"

# Spike output target. The real results bucket (sector-in-brief-api-results-stg
# + 30-day lifecycle) is an IaC deliverable AFTER the host decision; writing it
# ad-hoc here would fight CloudFormation later. So the spike materializes to a
# disposable prefix in the (writable, soon-to-sunset) legacy stg bucket.
OUT_BUCKET = "nccs-dataexplorer-stg"
OUT_KEY    = f"phase0-spike/result_{TAX_YEAR}_990_{STATE}.csv"

PROJECTION = """
    b.ein,
    b.org_name_display,
    b.org_addr_city,
    b.geo_state_abbr,
    b.ntee_common_code,
    c.total_revenue,
    c.total_assets_eoy,
    c.total_net_assets_eoy
"""


def connect():
    cr = boto3.Session().get_credentials().get_frozen_credentials()
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        "CREATE SECRET s (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION 'us-east-1');",
        [cr.access_key, cr.secret_key, cr.token],
    )
    return con


def main():
    con = connect()
    # EIN-join at query time; filter on the geocoded (canonical) state.
    where = "WHERE b.geo_state_abbr = ?" if STATE else ""
    sql = f"SELECT {PROJECTION} FROM read_parquet('{CORE}') c " \
          f"JOIN read_parquet('{BMF}') b ON c.ein = b.ein {where}"
    params = [STATE] if STATE else []

    t0 = time.time()
    n = con.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()[0]
    t_query = time.time() - t0
    print(f"[query]      {TAX_YEAR}/990 JOIN bmf-geocoded WHERE geo_state_abbr={STATE!r}")
    print(f"[query]      rows = {n:,}   ({t_query:.2f}s to count)")

    # ADR 0026 pattern B: stream result straight to S3, never through the process.
    t0 = time.time()
    con.execute(f"COPY ({sql}) TO 's3://{OUT_BUCKET}/{OUT_KEY}' (FORMAT csv, HEADER);", params)
    t_copy = time.time() - t0

    s3 = boto3.client("s3", region_name="us-east-1")
    size = s3.head_object(Bucket=OUT_BUCKET, Key=OUT_KEY)["ContentLength"]
    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": OUT_BUCKET, "Key": OUT_KEY}, ExpiresIn=3600
    )
    print(f"[materialize] s3://{OUT_BUCKET}/{OUT_KEY}")
    print(f"[materialize] {size/1024/1024:.2f} MB written in {t_copy:.2f}s")
    print(f"[presign]     {url[:90]}...")

    print("\n" + json.dumps({
        "tax_year": TAX_YEAR, "state": STATE, "rows": n,
        "result_bytes": size, "query_s": round(t_query, 2), "copy_s": round(t_copy, 2),
    }))


if __name__ == "__main__":
    main()
