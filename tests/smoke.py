"""
Post-deploy smoke suite — asserts real behavior of a DEPLOYED stack (the gate
before promotion). Run after `sam deploy`:

  RESULTS_BUCKET=sector-in-brief-api-results-stg AWS_PROFILE=thiya \
    python tests/smoke.py

Env:
  QUERY_FN       Lambda to invoke (default sector-in-brief-api-query-stg). It
                 routes /data + /download, so one function covers the suite.
  RESULTS_BUCKET results bucket (required) — for url/object/telemetry checks.
  SMOKE_EMAIL    if set, also tests the SES receipt (sends one real email there).
  SMOKE_MODE     'deployed' (default, boto3 invoke) | 'local' (import the handler).

Exit 0 iff every check passes; cleans up its own result/registry/log objects.
"""
import os, sys, io, csv, json, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = os.environ["RESULTS_BUCKET"]
QUERY_FN = os.environ.get("QUERY_FN", "sector-in-brief-api-query-stg")
SMOKE_EMAIL = os.environ.get("SMOKE_EMAIL")
MODE = os.environ.get("SMOKE_MODE", "deployed")

s3 = boto3.client("s3", region_name=REGION)
if MODE == "local":
    from query.query import lambda_handler as _handler
else:
    _lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=900, retries={"max_attempts": 0}))


def call(payload):
    if MODE == "local":
        return _handler(payload, type("C", (), {"aws_request_id": "smoke"})())
    r = _lam.invoke(FunctionName=QUERY_FN, Payload=json.dumps(payload).encode())
    return json.loads(r["Payload"].read())


RESULTS, JIDS = [], []


def check(name, cond, detail=""):
    RESULTS.append(bool(cond))
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + ("" if cond else f"   <- {detail}"))
    return bool(cond)


def body(resp):
    return json.loads(resp["body"])


def http_ok(url):
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return r.status == 200
    except Exception as e:  # noqa: BLE001
        return False


def rows_of(jid):
    data = s3.get_object(Bucket=BUCKET, Key=f"results/{jid}.csv")["Body"].read().decode()
    return list(csv.reader(io.StringIO(data)))


print(f"smoke: mode={MODE} fn={QUERY_FN} bucket={BUCKET}\n")

# 1) happy path (small state for speed) + presigned URLs resolve
r = call({"tax_years": [2019], "forms": ["990"], "columns": ["ein", "total_revenue"],
          "filters": {"geo_state_abbr": ["RI"]}})
if check("data: 200", r.get("statusCode") == 200, str(r)[:200]):
    b = body(r); JIDS.append(b["job_id"])
    check("data: row_count > 0", b["row_count"] > 0)
    check("data: result URL resolves (S3 200)", http_ok(b["result"]["url"]))
    check("data: dictionary URL resolves (S3 200)", http_ok(b["data_dictionary"]["url"]))

# 2) validation
check("validate: unknown column -> 400", call({"tax_years": [2019], "columns": ["nope"]}).get("statusCode") == 400)
check("validate: missing tax_years -> 400", call({"columns": ["ein"]}).get("statusCode") == 400)

# 3) durable download + re-materialize on sweep + bad job
if JIDS:
    jid = JIDS[0]
    check("download: 302 redirect", call({"download": jid}).get("statusCode") == 302)
    s3.delete_object(Bucket=BUCKET, Key=f"results/{jid}.csv")          # simulate lifecycle sweep
    d2 = call({"download": jid})
    back = True
    try:
        s3.head_object(Bucket=BUCKET, Key=f"results/{jid}.csv")
    except Exception:
        back = False
    check("download: re-materializes after sweep", d2.get("statusCode") == 302 and back)
check("download: bad job_id -> 404", call({"download": "not-a-uuid"}).get("statusCode") == 404)

# 4) geo crosswalk (slice 5): filter by a small REAL column (pushes down -> fast),
# and assert the crosswalk-derived columns come back correct. NB: do NOT filter by
# a derived column (census_region/geo_county_fips) here — that can't push down and
# runs the crosswalk joins over the whole BMF (see the region-filter perf follow-up).
g = call({"tax_years": [2019], "forms": ["990"], "columns": ["ein", "geo_county_fips", "census_region"],
          "filters": {"geo_state_abbr": ["RI"]}})
if check("geo: 200", g.get("statusCode") == 200, str(g)[:200]):
    gb = body(g); JIDS.append(gb["job_id"])
    rows = rows_of(gb["job_id"]); h = rows[0]
    regs = {x[h.index("census_region")] for x in rows[1:300]}
    check("geo: census_region derived correctly (RI -> Northeast)", regs == {"Northeast"}, str(regs))
    check("geo: geo_county_fips populated from crosswalk",
          any(x[h.index("geo_county_fips")] for x in rows[1:300]))

# 4b) census_region filter expands to states (slice 5.1) — intersect with RI to stay tiny
gr = call({"tax_years": [2019], "forms": ["990"], "columns": ["ein", "geo_state_abbr", "census_region"],
           "filters": {"census_region": ["Northeast"], "geo_state_abbr": ["RI"]}})
if check("geo: region filter (5.1) -> 200", gr.get("statusCode") == 200, str(gr)[:200]):
    grb = body(gr); JIDS.append(grb["job_id"])
    rows = rows_of(grb["job_id"]); h = rows[0]
    sts = {x[h.index("geo_state_abbr")] for x in rows[1:300]}
    check("geo: census_region filter resolves to states (RI ∩ Northeast = RI)", sts == {"RI"}, str(sts))

# 4c) org_type derived column (mirrors the producer's 501(c)(3) PC/PF split)
ot = call({"tax_years": [2019], "forms": ["990"], "columns": ["ein", "subsection_code", "foundation_code"],
           "filters": {"geo_state_abbr": ["DC"], "org_type": ["501(c)(3) Private Foundations"]}})
if check("org_type: PF filter -> 200", ot.get("statusCode") == 200, str(ot)[:200]):
    otb = body(ot); JIDS.append(otb["job_id"]); rows = rows_of(otb["job_id"]); h = rows[0]
    si, fi = h.index("subsection_code"), h.index("foundation_code")
    check("org_type: PF = subsection 3 + foundation 2/3/4",
          all(r[si] == "3" and r[fi] in ("2", "3", "4") for r in rows[1:]))

# 5) estimate: no materialization
e = body(call({"tax_years": [2019], "forms": ["990"], "columns": ["ein"],
               "filters": {"geo_state_abbr": ["RI"]}, "estimate": True}))
check("estimate: row_count present, no result materialized", "row_count" in e and "result" not in e)

# 6) telemetry landed
day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
check("telemetry: events written to logs/queries/",
      s3.list_objects_v2(Bucket=BUCKET, Prefix=f"logs/queries/{day}/").get("KeyCount", 0) > 0)

# 7) email receipt (opt-in)
if SMOKE_EMAIL:
    em = body(call({"tax_years": [2019], "forms": ["990"], "columns": ["ein"],
                    "filters": {"geo_state_abbr": ["RI"]}, "email": SMOKE_EMAIL}))
    JIDS.append(em["job_id"])
    check("email: receipt sent", (em.get("email") or {}).get("status") == "sent", str(em.get("email")))

# ---- cleanup: this run's results/registry + its telemetry events ----
for jid in JIDS:
    for k in [f"results/{jid}.csv", f"results/{jid}_dictionary.csv", f"requests/{jid}.json"]:
        try:
            s3.delete_object(Bucket=BUCKET, Key=k)
        except Exception:
            pass
for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=f"logs/queries/{day}/").get("Contents", []):
    try:
        rec = json.loads(s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read())
        if rec.get("job_id") in JIDS:
            s3.delete_object(Bucket=BUCKET, Key=o["Key"])
    except Exception:
        pass

passed = sum(RESULTS)
print(f"\n{passed}/{len(RESULTS)} checks passed")
sys.exit(0 if passed == len(RESULTS) else 1)
