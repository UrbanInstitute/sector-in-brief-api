"""
Phase-0 in-region latency harness (ADR 0008 host decision).

Run this FROM INSIDE us-east-1 (CloudShell or a throwaway EC2) to measure the
true DuckDB-COPY-to-S3 throughput the eventual Lambda/App-Runner host will see.
The local-WSL spike measured ~1.5 MB/s, but that crossed the public internet;
this gives the real in-region number that decides the Lambda-vs-App-Runner line.

Designed to be run BY THE OPERATOR under their own identity (not by an automated
assistant session) — it makes only ordinary S3 data-plane calls (read nccsdata,
write+delete a throwaway prefix). No EC2/IAM/SSM/control-plane calls.

Usage:
  pip install duckdb boto3
  python measure_latency_inregion.py [max_tier] [mem_limit]
    max_tier : small | medium | large   (default: medium — safe for CloudShell)
    mem_limit: DuckDB memory cap, e.g. 1.5GB (CloudShell) or 12GB (EC2). default 1.5GB

Paste the final JSON block back to the assistant.
"""
import sys, time, json, boto3, duckdb

NCCS = "s3://nccsdata"
BMF  = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded.parquet"
CORE = lambda glob: f"{NCCS}/processed/core/{glob}/990/core_*_990.parquet"
OUT_BUCKET = "nccs-dataexplorer-stg"          # writable; soon-to-sunset legacy bucket
OUT_PREFIX = "phase0-inregion/"

PROJ = ("b.ein, b.org_name_display, b.org_addr_city, b.geo_state_abbr, "
        "b.ntee_common_code, c.total_revenue, c.total_assets_eoy, c.total_net_assets_eoy")

# Query sweep mapped onto the real result-size distribution.
TIERS = {
    "small":  dict(core="2019",  where="b.geo_state_abbr = 'CA'", note="~p50: one state, one year"),
    "medium": dict(core="*",     where="1=1",                     note="~p75-p90: all years, all states (narrow)"),
    "large":  dict(core="*",     where="1=1", wide=True,          note="tail: all years, wide projection (may OOM small hosts)"),
}
ORDER = ["small", "medium", "large"]


def detect_region():
    """region for the in-region check; bare SSM shells have no configured region."""
    r = boto3.Session().region_name
    if r:
        return r
    try:  # IMDSv2 -> availability zone -> region
        import urllib.request as u
        tok = u.urlopen(u.Request("http://169.254.169.254/latest/api/token", method="PUT",
              headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}), timeout=1).read().decode()
        az = u.urlopen(u.Request("http://169.254.169.254/latest/meta-data/placement/availability-zone",
              headers={"X-aws-ec2-metadata-token": tok}), timeout=1).read().decode()
        return az[:-1]
    except Exception:
        return "unknown"


def connect(mem_limit):
    cr = boto3.Session().get_credentials().get_frozen_credentials()
    con = duckdb.connect()
    # SSM root shells have an empty $HOME, so DuckDB can't find a place to cache
    # the httpfs extension; pin it explicitly.
    con.execute("SET home_directory='/tmp';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET s (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION 'us-east-1');",
                [cr.access_key, cr.secret_key, cr.token])
    con.execute(f"SET memory_limit='{mem_limit}'; SET temp_directory='/tmp/duckdb_spill';")
    return con


def run_tier(con, name, s3):
    t = TIERS[name]
    proj = PROJ + (", c.* " if t.get("wide") else "")
    sql = (f"SELECT {proj} FROM read_parquet('{CORE(t['core'])}') c "
           f"JOIN read_parquet('{BMF}') b ON c.ein = b.ein WHERE {t['where']}")
    key = f"{OUT_PREFIX}{name}.csv"
    t0 = time.time(); rows = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]; t_count = time.time() - t0
    t0 = time.time(); con.execute(f"COPY ({sql}) TO 's3://{OUT_BUCKET}/{key}' (FORMAT csv, HEADER);"); t_copy = time.time() - t0
    size = s3.head_object(Bucket=OUT_BUCKET, Key=key)["ContentLength"]
    s3.delete_object(Bucket=OUT_BUCKET, Key=key)
    mbps = (size / 1024 / 1024) / t_copy if t_copy else 0
    r = dict(tier=name, note=t["note"], rows=rows, bytes=size,
             count_s=round(t_count, 1), copy_s=round(t_copy, 1), copy_MB_s=round(mbps, 1))
    print(f"  {name:7s} rows={rows:>9,}  {size/1024/1024:>8.1f} MB  count={t_count:5.1f}s  "
          f"copy={t_copy:6.1f}s  -> {mbps:5.1f} MB/s")
    return r


def main():
    max_tier = sys.argv[1] if len(sys.argv) > 1 else "medium"
    mem = sys.argv[2] if len(sys.argv) > 2 else "1.5GB"
    sts = boto3.client("sts"); ident = sts.get_caller_identity()
    region = detect_region()
    print(f"identity: {ident['Arn']}")
    print(f"region:   {region}   (MUST be us-east-1 for an in-region number)")
    print(f"duckdb {duckdb.__version__}  mem_limit={mem}  max_tier={max_tier}\n")
    s3 = boto3.client("s3", region_name="us-east-1")
    con = connect(mem)
    results = []
    for name in ORDER[: ORDER.index(max_tier) + 1]:
        try:
            results.append(run_tier(con, name, s3))
        except Exception as e:
            print(f"  {name:7s} FAILED: {type(e).__name__}: {str(e)[:140]}")
            results.append(dict(tier=name, failed=f"{type(e).__name__}: {str(e)[:140]}"))
    print("\n=== PASTE THIS BACK ===")
    print(json.dumps(dict(region=region, duckdb=duckdb.__version__, mem_limit=mem, results=results)))


if __name__ == "__main__":
    main()
