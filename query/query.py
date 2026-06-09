"""
sector-in-brief-api — /data export + durable /download handler (slices 1-2,
ADR 0008 / 0026).

POST /data: validate the request against the live parquet schema, join the
requested CORE form tiers to bmf-master-geocoded on EIN (ADR 0016), materialize
the result + a merged data dictionary to the results bucket (pattern B), write a
request-registry sidecar, and return presigned URLs.

GET /download/{job_id} (ADR 0026 §2): the DURABLE link. Resolve the registry; if
the result object still exists, 302 to a freshly-issued presigned URL; if the
30-day lifecycle has swept it, re-run the query from the stored params and 302 to
the fresh result. The registry (`requests/`) lives on a longer clock than the
result (`results/`, 30-day) — the lifecycle rule is scoped to `results/` only.

Security: column names are whitelisted against the live schema before they touch
SQL (filter values are bound params); job_id is shape-checked before it touches an
S3 key. Interface: ../openapi.yaml.
"""
import os, re, json, base64, uuid, boto3, duckdb
from datetime import datetime, timezone

NCCS = "s3://nccsdata"
BMF = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded.parquet"
BMF_DICT = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded_data_dictionary.csv"
RESULTS_BUCKET = os.environ["RESULTS_BUCKET"]
URL_TTL = int(os.environ.get("URL_TTL_SECONDS", "3600"))   # <= object lifetime (30d)
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")              # verified SES sender; receipt is sent if set
DOWNLOAD_BASE_URL = os.environ.get("DOWNLOAD_BASE_URL", "")  # public /download Function URL (for the email link)
DOWNLOAD_ONLY = bool(os.environ.get("DOWNLOAD_ONLY"))     # public download function: refuse /data
ALL_FORMS = ["990", "990ez", "990pf", "990combined"]
EXT = {"csv": "csv", "parquet": "parquet"}
JOB_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class BadRequest(Exception):
    pass


# ---- path builders -----------------------------------------------------------
def _core_parquets(years, forms):
    return [f"{NCCS}/processed/core/{y}/{f}/core_{y}_{f}.parquet" for y in years for f in forms]


def _core_dicts(years, forms):
    y = max(years)  # descriptions are stable across years; one representative vintage
    return [f"{NCCS}/processed/core/{y}/{f}/core_{y}_{f}_dictionary.csv" for f in forms]


def _sql_list(paths):
    return "[" + ", ".join(f"'{p}'" for p in paths) + "]"


# ---- duckdb ------------------------------------------------------------------
def _con():
    cr = boto3.Session().get_credentials().get_frozen_credentials()
    con = duckdb.connect()
    con.execute("SET home_directory='/tmp'; SET temp_directory='/tmp/duckdb_spill';")
    con.execute("SET enable_progress_bar=false;")   # no TTY in Lambda — keeps CloudWatch clean
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET s (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION 'us-east-1');",
                [cr.access_key, cr.secret_key, cr.token])
    return con


def _column_sources(con, core_paths):
    """Map every queryable column to its alias: 'c' (core) wins overlaps, else 'b' (bmf)."""
    core_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_sql_list(core_paths)}, union_by_name=true)").fetchall()]
    bmf_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{BMF}')").fetchall()]
    src = {c: "b" for c in bmf_cols}
    src.update({c: "c" for c in core_cols})   # core wins on overlap (e.g. ein)
    return src


# ---- request validation / sql (pure) -----------------------------------------
def _parse_body(event):
    if isinstance(event, dict) and "body" in event:           # Function URL / API GW
        body = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode()
        return json.loads(body)
    return event or {}                                        # direct invoke (tests)


def _validate(req, src):
    years = req.get("tax_years")
    if not isinstance(years, list) or not years:
        raise BadRequest("tax_years must be a non-empty list of years")
    forms = req.get("forms", ALL_FORMS)
    if not all(f in ALL_FORMS for f in forms):
        raise BadRequest(f"forms must be a subset of {ALL_FORMS}")
    cols = list(dict.fromkeys(req.get("columns") or []))
    if not cols:
        raise BadRequest("columns must be a non-empty list")
    if "ein" not in cols:
        cols.insert(0, "ein")
    fmt = req.get("format", "csv")
    if fmt not in EXT:
        raise BadRequest("format must be 'csv' or 'parquet'")
    filters = req.get("filters") or {}
    unknown = [c for c in cols if c not in src] + [k for k in filters if k not in src]
    if unknown:
        raise BadRequest(f"unknown column(s): {sorted(set(unknown))}")
    return years, forms, cols, filters, fmt


def _build_sql(cols, filters, src, core_paths):
    select = ", ".join(f'{src[c]}."{c}" AS "{c}"' for c in cols)
    where, params = "", []
    if filters:
        clauses = []
        for k, vals in filters.items():
            vals = vals if isinstance(vals, list) else [vals]
            clauses.append(f'{src[k]}."{k}" IN ({", ".join(["?"] * len(vals))})')
            params += [str(v) for v in vals]
        where = "WHERE " + " AND ".join(clauses)
    sql = (f"SELECT {select} "
           f"FROM read_parquet({_sql_list(core_paths)}, union_by_name=true) c "
           f'JOIN read_parquet(\'{BMF}\') b ON c."ein" = b."ein" {where}')
    return sql, params


def _dictionary_sql(cols, src, core_dicts):
    core_cols = [c for c in cols if src[c] == "c"]
    bmf_cols = [c for c in cols if src[c] == "b"]
    parts, params = [], []
    if core_cols:
        parts.append(
            "SELECT harmonized_name AS column, 'core' AS source, any_value(description) AS description, "
            "any_value(data_type) AS data_type, any_value(null_pct) AS null_pct "
            f"FROM read_csv({_sql_list(core_dicts)}, union_by_name=true) "
            f"WHERE harmonized_name IN ({', '.join(['?'] * len(core_cols))}) GROUP BY harmonized_name")
        params += core_cols
    if bmf_cols:
        parts.append(
            "SELECT column_name AS column, 'bmf-geocoded' AS source, description, "
            "type AS data_type, null_pct "
            f"FROM read_csv('{BMF_DICT}') WHERE column_name IN ({', '.join(['?'] * len(bmf_cols))})")
        params += bmf_cols
    return " UNION ALL ".join(parts), params


# ---- plan / materialize ------------------------------------------------------
def _plan(con, req):
    years = req.get("tax_years") or []
    forms = req.get("forms", ALL_FORMS)
    if not isinstance(years, list) or not years:
        raise BadRequest("tax_years must be a non-empty list of years")
    if not all(f in ALL_FORMS for f in forms):
        raise BadRequest(f"forms must be a subset of {ALL_FORMS}")
    core_paths = _core_parquets(years, forms)
    src = _column_sources(con, core_paths)
    years, forms, cols, filters, fmt = _validate(req, src)
    return dict(years=years, forms=forms, cols=cols, filters=filters, fmt=fmt, src=src, core_paths=core_paths)


def _materialize(con, plan, job_id, s3):
    sql, params = _build_sql(plan["cols"], plan["filters"], plan["src"], plan["core_paths"])
    fmt = plan["fmt"]
    result_key = f"results/{job_id}.{EXT[fmt]}"
    rows = con.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()[0]
    con.execute(f"COPY ({sql}) TO 's3://{RESULTS_BUCKET}/{result_key}' (FORMAT {fmt}, HEADER);", params)
    dsql, dparams = _dictionary_sql(plan["cols"], plan["src"], _core_dicts(plan["years"], plan["forms"]))
    dict_key = f"results/{job_id}_dictionary.csv"
    con.execute(f"COPY ({dsql}) TO 's3://{RESULTS_BUCKET}/{dict_key}' (FORMAT csv, HEADER);", dparams)
    size = s3.head_object(Bucket=RESULTS_BUCKET, Key=result_key)["ContentLength"]
    return dict(row_count=rows, result_key=result_key, dict_key=dict_key, bytes=size,
                columns=len(plan["cols"]), format=fmt)


# ---- responses ---------------------------------------------------------------
def _presign(s3, key):
    return s3.generate_presigned_url("get_object",
                                     Params={"Bucket": RESULTS_BUCKET, "Key": key}, ExpiresIn=URL_TTL)


def _resp(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "body": json.dumps(body)}


def _exists(s3, key):
    try:
        s3.head_object(Bucket=RESULTS_BUCKET, Key=key)
        return True
    except Exception:
        return False


# ---- email receipt (ADR 0026 §3, default-on) ---------------------------------
CONTACT = "tpoongundranar@urban.org"


def _send_receipt(email, info):
    """Best-effort receipt (ADR 0026 §3): a failed send must not fail the export.
    info: data_url, dict_url, row_count, format, tax_years, forms, columns, filters, job_id."""
    if not (email and SENDER_EMAIL):
        return "skipped:no_sender"
    flt = info["filters"]
    flt_str = "; ".join(f"{k}: {', '.join(map(str, v))}" for k, v in flt.items()) if flt else "none"
    summary = [("Rows", f"{info['row_count']:,}"), ("Format", info["format"].upper()),
               ("Tax years", ", ".join(map(str, info["tax_years"]))),
               ("Form types", ", ".join(info["forms"])),
               ("Columns", ", ".join(info["columns"])), ("Filters", flt_str),
               ("Reference ID", info["job_id"])]
    text = ("Your NCCS data export is ready.\n\n"
            f"Download your data:\n{info['data_url']}\n\n"
            f"Download the data dictionary (explains each column):\n{info['dict_url']}\n\n"
            "Your export:\n" + "\n".join(f"  {k}: {v}" for k, v in summary) + "\n\n"
            "These links stay valid: the files are kept for 30 days, and the links "
            f"regenerate them on demand after that.\n\nQuestions? {CONTACT}")
    rows = "".join(f"<tr><td><b>{k}</b>&nbsp;</td><td>{v}</td></tr>" for k, v in summary)
    html = ("<p>Your NCCS data export is ready.</p>"
            f'<p><a href="{info["data_url"]}">Download your data</a></p>'
            f'<p><a href="{info["dict_url"]}">Download the data dictionary</a> (explains each column)</p>'
            f"<p><b>Your export</b></p><table>{rows}</table>"
            "<p>These links stay valid — the files are kept for 30 days and the links "
            "regenerate them on demand after that.</p>"
            f'<p>Questions? <a href="mailto:{CONTACT}">{CONTACT}</a></p>')
    try:
        boto3.client("ses", region_name="us-east-1").send_email(
            Source=SENDER_EMAIL, Destination={"ToAddresses": [email]},
            Message={"Subject": {"Data": "Your NCCS data export is ready"},
                     "Body": {"Text": {"Data": text}, "Html": {"Data": html}}})
        return "sent"
    except Exception as e:  # noqa: BLE001
        return f"failed:{type(e).__name__}"


# ---- routes ------------------------------------------------------------------
def _create_export(event, s3):
    req = _parse_body(event)
    con = _con()
    plan = _plan(con, req)
    job_id = str(uuid.uuid4())
    m = _materialize(con, plan, job_id, s3)
    email = req.get("email")
    registry = {
        "job_id": job_id,
        "request": {"tax_years": plan["years"], "forms": plan["forms"],
                    "columns": plan["cols"], "filters": plan["filters"], "format": plan["fmt"]},
        "email": email,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "result_key": m["result_key"],
    }
    s3.put_object(Bucket=RESULTS_BUCKET, Key=f"requests/{job_id}.json",
                  Body=json.dumps(registry), ContentType="application/json")
    base = DOWNLOAD_BASE_URL.rstrip("/")
    data_url = f"{base}/download/{job_id}" if base else f"/download/{job_id}"
    dict_url = f"{data_url}?kind=dictionary"
    receipt = _send_receipt(email, {
        "data_url": data_url, "dict_url": dict_url, "row_count": m["row_count"],
        "format": m["format"], "tax_years": plan["years"], "forms": plan["forms"],
        "columns": plan["cols"], "filters": plan["filters"], "job_id": job_id}) if email else None
    return _resp(200, {
        "job_id": job_id,
        "row_count": m["row_count"],
        "result": {"format": m["format"], "bytes": m["bytes"],
                   "url": _presign(s3, m["result_key"]), "expires_in_seconds": URL_TTL},
        "data_dictionary": {"url": _presign(s3, m["dict_key"]), "columns": m["columns"]},
        "download_path": f"/download/{job_id}",
        "download_url": data_url if base else None,
        "dictionary_download_url": dict_url if base else None,
        "email": {"to": email, "status": receipt} if email else None,
    })


def _download(job_id, s3, kind="result"):
    if not JOB_RE.match(job_id or ""):
        return _resp(404, {"error": "unknown_job"})
    try:
        registry = json.loads(s3.get_object(Bucket=RESULTS_BUCKET, Key=f"requests/{job_id}.json")["Body"].read())
    except Exception:
        return _resp(404, {"error": "unknown_job"})
    target_key = (f"results/{job_id}_dictionary.csv" if kind == "dictionary"
                  else registry["result_key"])
    if not _exists(s3, target_key):                  # swept by the 30-day lifecycle -> re-run
        con = _con()                                  # re-materialize regenerates BOTH result + dict
        plan = _plan(con, registry["request"])
        _materialize(con, plan, job_id, s3)
    return {"statusCode": 302, "headers": {"Location": _presign(s3, target_key)}}


def lambda_handler(event, context):
    s3 = boto3.client("s3", region_name="us-east-1")
    try:
        rc = event.get("requestContext", {}) if isinstance(event, dict) else {}
        method = rc.get("http", {}).get("method")
        raw = event.get("rawPath", "") if isinstance(event, dict) else ""
        is_download = (method == "GET" and raw.startswith("/download/"))
        direct_download = isinstance(event, dict) and event.get("download")
        if is_download:
            kind = (event.get("queryStringParameters") or {}).get("kind", "result")
            return _download(raw[len("/download/"):], s3, kind)
        if direct_download:                                     # direct-invoke test hook
            return _download(event["download"], s3, event.get("kind", "result"))
        if DOWNLOAD_ONLY:                                       # public download function: no /data
            return _resp(403, {"error": "forbidden", "detail": "this endpoint only serves downloads"})
        return _create_export(event, s3)
    except BadRequest as e:
        return _resp(400, {"error": "validation_error", "detail": str(e)})
    except Exception as e:  # noqa: BLE001 — surface the failure class to the caller
        return _resp(500, {"error": type(e).__name__, "detail": str(e)[:300]})
