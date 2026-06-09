"""
Build step 0 — Check 2 probe (gates the Lambda-first host decision).

Runs the heaviest realistic query (WIDE projection — all ~250 core columns —
across all 990 tax years, all states) as a DuckDB-on-parquet EIN join INSIDE
Lambda, materializing to the real results bucket (ADR 0026 pattern B). The
question Phase-0 left open: does it complete within Lambda's 10 GB memory
ceiling, and at what in-region S3-write throughput (the deferred Check 1)?

Read the answer from the Lambda REPORT line (Max Memory Used, Duration) plus the
returned JSON (rows, bytes, copy throughput). If it OOMs, Lambda-first is refuted
for the wide tail and that slice of the distribution pivots to an async worker.

NOT the production handler — a probe. Credentials come from the execution role
via boto3 -> an explicit DuckDB S3 secret (DuckDB's own credential chain can't
use an assumed role).
"""
import os, time, json, boto3, duckdb

NCCS = "s3://nccsdata"
BMF  = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded.parquet"
RESULTS_BUCKET = os.environ["RESULTS_BUCKET"]

BMF_COLS = ("b.ein, b.org_name_display, b.org_addr_city, "
            "b.geo_state_abbr, b.ntee_common_code")


def _con():
    cr = boto3.Session().get_credentials().get_frozen_credentials()
    con = duckdb.connect()
    # Lambda: $HOME is unset, so pin DuckDB's home; spill to the 10 GB ephemeral
    # /tmp (set EphemeralStorage in the template).
    con.execute("SET home_directory='/tmp'; SET temp_directory='/tmp/duckdb_spill';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET s (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION 'us-east-1');",
                [cr.access_key, cr.secret_key, cr.token])
    return con


def lambda_handler(event, context):
    event = event or {}
    core_glob = event.get("core_glob", "*")        # all tax years by default
    state     = event.get("state")                  # None -> all states
    wide      = event.get("wide", True)             # wide projection = the memory stressor
    core = f"{NCCS}/processed/core/{core_glob}/990/core_*_990.parquet"
    # EXCLUDE the duplicate join key so the wide projection has no colliding 'ein'.
    proj = (f"{BMF_COLS}, c.* EXCLUDE (ein)" if wide
            else f"{BMF_COLS}, c.total_revenue, c.total_assets_eoy, c.total_net_assets_eoy")
    where = "WHERE b.geo_state_abbr = ?" if state else ""
    params = [state] if state else []
    sql = (f"SELECT {proj} FROM read_parquet('{core}') c "
           f"JOIN read_parquet('{BMF}') b ON c.ein = b.ein {where}")

    con = _con()
    key = f"check2/{context.aws_request_id}.csv"
    t0 = time.time()
    rows = con.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()[0]
    t_count = time.time() - t0
    t0 = time.time()
    con.execute(f"COPY ({sql}) TO 's3://{RESULTS_BUCKET}/{key}' (FORMAT csv, HEADER);", params)
    t_copy = time.time() - t0

    s3 = boto3.client("s3", region_name="us-east-1")
    size = s3.head_object(Bucket=RESULTS_BUCKET, Key=key)["ContentLength"]
    s3.delete_object(Bucket=RESULTS_BUCKET, Key=key)
    out = {"wide": wide, "core_glob": core_glob, "state": state, "rows": rows,
           "result_bytes": size, "count_s": round(t_count, 1), "copy_s": round(t_copy, 1),
           "copy_MB_s": round((size / 1024 / 1024) / t_copy, 1) if t_copy else None,
           "lambda_mem_limit_mb": context.memory_limit_in_mb}
    print("CHECK2_RESULT " + json.dumps(out))
    return out
