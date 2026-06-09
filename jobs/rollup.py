"""
Monthly usage rollup (ADR 0008 telemetry / contracts/usage-api.yml).

Aggregates the prior month's per-query NDJSON events
(s3://{RESULTS_BUCKET}/logs/queries/{YYYY-MM-DD}/*.ndjson, written by the API
handler) into the contracted parquet artifact
s3://nccsdata/usage/api/{YYYY_MM}/queries.parquet plus an ADR-0014 _manifest.json.
Idempotent: re-running a past month overwrites that vintage.

Scheduled on the 1st of each month (EventBridge). An event may pass
{"month": "YYYY_MM"} to backfill a specific month.
"""
import os, json, hashlib, boto3, duckdb
from datetime import datetime, timezone

RESULTS_BUCKET = os.environ["RESULTS_BUCKET"]
USAGE_BUCKET = os.environ.get("USAGE_BUCKET", "nccsdata")
USAGE_PREFIX = os.environ.get("USAGE_PREFIX", "usage/api")   # -> {prefix}/{YYYY_MM}/queries.parquet

AGG_SQL = """
WITH e AS (SELECT *, ts::timestamp AS t FROM read_json(?, format='newline_delimited', union_by_name=true))
SELECT
  date_trunc('month', t)::date                                              AS vintage_month,
  t::date                                                                    AS day,
  count(*)            FILTER (WHERE event='export_materialized')             AS n_queries,
  count(*)            FILTER (WHERE event='request_created')                 AS n_requests,
  count(DISTINCT requester) FILTER (WHERE event='request_created')           AS n_unique_users,
  count(*)            FILTER (WHERE event='download')                        AS n_downloads,
  count(*)            FILTER (WHERE event='export_materialized' AND success=false) AS n_failures,
  CAST(quantile_cont(bytes, 0.5)  FILTER (WHERE event='export_materialized' AND success=true) AS BIGINT) AS bytes_returned_p50,
  CAST(quantile_cont(bytes, 0.95) FILTER (WHERE event='export_materialized' AND success=true) AS BIGINT) AS bytes_returned_p95,
  CAST(quantile_cont(duration_ms, 0.5)  FILTER (WHERE event='export_materialized' AND success=true) AS BIGINT) AS duration_ms_p50,
  CAST(quantile_cont(duration_ms, 0.95) FILTER (WHERE event='export_materialized' AND success=true) AS BIGINT) AS duration_ms_p95
FROM e GROUP BY 1, 2 ORDER BY 2
"""


def _prev_month(now):
    return (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)


def _con():
    cr = boto3.Session().get_credentials().get_frozen_credentials()
    con = duckdb.connect()
    con.execute("SET home_directory='/tmp'; SET temp_directory='/tmp'; SET enable_progress_bar=false;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET s (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION 'us-east-1');",
                [cr.access_key, cr.secret_key, cr.token])
    return con


def lambda_handler(event, context):
    event = event or {}
    if event.get("month"):
        y, m = (int(x) for x in event["month"].split("_"))
    else:
        y, m = _prev_month(datetime.now(timezone.utc))
    ym = f"{y}_{m:02d}"
    s3 = boto3.client("s3", region_name="us-east-1")

    # any events for the month?
    day_prefix = f"logs/queries/{y}-{m:02d}-"
    src = s3.list_objects_v2(Bucket=RESULTS_BUCKET, Prefix=day_prefix).get("KeyCount", 0)
    if not src:
        return {"status": "no_events", "month": ym}

    glob = f"s3://{RESULTS_BUCKET}/{day_prefix}*/*.ndjson"
    con = _con()
    local = "/tmp/queries.parquet"
    con.execute(f"COPY ({AGG_SQL.replace('?', repr(glob))}) TO '{local}' (FORMAT parquet, COMPRESSION zstd);")
    rows = con.execute("SELECT count(*) FROM read_parquet(?)", [local]).fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [local]).fetchall()]

    size = os.path.getsize(local)
    sha = hashlib.sha256(open(local, "rb").read()).hexdigest()
    key = f"{USAGE_PREFIX}/{ym}/queries.parquet"
    s3.upload_file(local, USAGE_BUCKET, key)

    manifest = {
        "vintage": f"v{y}.{m:02d}",
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git_sha": os.environ.get("GIT_SHA", "NA"),
        "inputs": [{"uri": glob, "n_source_objects": src}],
        "files": {"queries.parquet": {"file": "queries.parquet", "sha256": sha,
                                      "bytes": size, "row_count": rows, "columns": cols}},
    }
    s3.put_object(Bucket=USAGE_BUCKET, Key=f"{USAGE_PREFIX}/{ym}/_manifest.json",
                  Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json")
    return {"status": "ok", "month": ym, "rows": rows, "bytes": size,
            "out": f"s3://{USAGE_BUCKET}/{key}", "source_objects": src}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None)))
