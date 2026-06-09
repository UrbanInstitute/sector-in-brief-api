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
import os, re, json, time, base64, uuid, hashlib, boto3, duckdb
from datetime import datetime, timezone

NCCS = "s3://nccsdata"
BMF = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded.parquet"
BMF_DICT = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded_data_dictionary.csv"

# Consumer-composed geographic crosswalk joins (ADR 0021/0023), mirroring
# sector-in-brief-data/R/{county_crosswalk,read_bmf,derive_dimensions}.R so the
# API's county/CBSA/region match the dashboard's panels.
XW = f"{NCCS}/crosswalks"
CXW_COUNTY = f"{XW}/county-fips/county_fips_crosswalk.parquet"   # join (geo_state_abbr, geo_county_raw)
CXW_CBSA = f"{XW}/cbsa/cbsa_crosswalk.parquet"                   # join county_fips
CXW_CT = f"{XW}/ct-planning-region/ct_planning_region_crosswalk.parquet"  # CT by %.2f (lat,lon)
CENSUS_REGION = {  # exact mirror of derive_census_region()
    "Northeast": ["CT", "ME", "MA", "NH", "RI", "VT", "NJ", "NY", "PA"],
    "Midwest": ["IL", "IN", "MI", "OH", "WI", "IA", "KS", "MN", "MO", "NE", "ND", "SD"],
    "South": ["DE", "DC", "FL", "GA", "MD", "NC", "SC", "VA", "WV", "AL", "KY", "MS",
              "TN", "AR", "LA", "OK", "TX"],
    "West": ["AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY", "AK", "CA", "HI", "OR", "WA"],
}
# Columns the crosswalk joins add (not in core/bmf dicts) -> static dictionary entries.
DERIVED_COLUMNS = {
    "geo_county_fips": ("Canonical county GEOID (5-char FIPS); filter by this, not name. "
                        "NA when the raw label was ambiguous/unresolved.", "character"),
    "geo_county_canonical": ("Canonical Census county name (NAMELSAD); CT resolved by coordinate.", "character"),
    "cbsa_code": ("OMB Core-Based Statistical Area code", "character"),
    "cbsa_title": ("Metro/Micro area title", "character"),
    "cbsa_type": ("Metropolitan or Micropolitan Statistical Area", "character"),
    "csa_code": ("Combined Statistical Area code", "character"),
    "csa_title": ("Combined Statistical Area title", "character"),
    "census_region": ("US Census region (Northeast/Midwest/South/West), derived from state.", "character"),
}
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


def _region_case():
    whens = " ".join(
        f"WHEN b.geo_state_abbr IN ({', '.join(repr(s) for s in states)}) THEN {region!r}"
        for region, states in CENSUS_REGION.items())
    return f"CASE {whens} END"


def _bmf_source():
    """bmf-master-geocoded enriched with the crosswalk-derived geo columns. Aliased `b`.
    Mirrors sector-in-brief-data/R/read_bmf.R: county-label join (ambiguous->NULL),
    CT override by %.2f coordinate, CBSA on the coalesced FIPS, region from state."""
    return f"""(
      SELECT b.*,
        COALESCE(cf.geo_county_fips, ct.geo_county_fips)             AS geo_county_fips,
        COALESCE(cf.geo_county_canonical, ct.geo_county_canonical)   AS geo_county_canonical,
        cb.cbsa_code, cb.cbsa_title, cb.cbsa_type, cb.csa_code, cb.csa_title,
        {_region_case()}                                            AS census_region
      FROM read_parquet('{BMF}') b
      LEFT JOIN read_parquet('{CXW_COUNTY}') cf
        ON b.geo_state_abbr = cf.geo_state_abbr AND b.geo_county = cf.geo_county_raw
      LEFT JOIN read_parquet('{CXW_CT}') ct
        ON b.geo_state_abbr = 'CT'
        AND printf('%.2f', b.geo_lat) = printf('%.2f', ct.lat2)
        AND printf('%.2f', b.geo_lon) = printf('%.2f', ct.lon2)
      LEFT JOIN read_parquet('{CXW_CBSA}') cb
        ON COALESCE(cf.geo_county_fips, ct.geo_county_fips) = cb.county_fips
    )"""


def _column_sources(con, core_paths):
    """Map every queryable column to its alias: 'c' (core) wins overlaps, else 'b'
    (enriched bmf, incl. the crosswalk-derived geo columns)."""
    core_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_sql_list(core_paths)}, union_by_name=true)").fetchall()]
    bmf_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {_bmf_source()} b").fetchall()]
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
           f'JOIN {_bmf_source()} b ON c."ein" = b."ein" {where}')
    return sql, params


def _dictionary_sql(cols, src, core_dicts):
    derived = [c for c in cols if c in DERIVED_COLUMNS]
    core_cols = [c for c in cols if src[c] == "c" and c not in DERIVED_COLUMNS]
    bmf_cols = [c for c in cols if src[c] == "b" and c not in DERIVED_COLUMNS]
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
    if derived:   # crosswalk-derived geo columns -> static entries (not in core/bmf dicts)
        vals = ", ".join(
            "('{c}', 'crosswalk', '{d}', '{t}')".format(
                c=c, d=DERIVED_COLUMNS[c][0].replace("'", "''"), t=DERIVED_COLUMNS[c][1])
            for c in derived)
        parts.append('SELECT "column", source, description, data_type, CAST(NULL AS DOUBLE) AS null_pct '
                     f'FROM (VALUES {vals}) AS t("column", source, description, data_type)')
    return " UNION ALL ".join(parts), params


# ---- plan / materialize ------------------------------------------------------
def _expand_region_filter(filters):
    """census_region is a DERIVED column (CASE over state), so a filter on it can't
    push down — DuckDB would enrich the whole BMF before filtering. Translate it to
    an equivalent geo_state_abbr filter (a real column) so it pushes to the parquet
    scan. AND semantics: if a state filter is also present, intersect."""
    if "census_region" not in filters:
        return filters
    unknown = [r for r in filters["census_region"] if r not in CENSUS_REGION]
    if unknown:
        raise BadRequest(f"unknown census_region(s): {unknown}; valid: {list(CENSUS_REGION)}")
    states = set().union(*(CENSUS_REGION[r] for r in filters["census_region"]))
    out = {k: v for k, v in filters.items() if k != "census_region"}
    if "geo_state_abbr" in out:
        states &= set(out["geo_state_abbr"])
    out["geo_state_abbr"] = sorted(states)
    return out


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
    filters = _expand_region_filter(filters)   # region -> states, so it pushes down (slice 5.1)
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


# ---- telemetry (ADR 0026 §4 / ADR 0008): per-query NDJSON to logs/queries/ ----
def _hash_requester(email):
    return hashlib.sha256(email.encode()).hexdigest()[:16] if email else None


def _log_event(s3, etype, payload):
    """Best-effort: one NDJSON object per event under a per-day prefix. The
    monthly rollup (slice 4b) aggregates these into the usage-api contract."""
    try:
        ts = datetime.now(timezone.utc)
        key = f"logs/queries/{ts:%Y-%m-%d}/{ts:%H%M%S}-{uuid.uuid4().hex[:8]}.ndjson"
        rec = {"event": etype, "ts": ts.isoformat(), **payload}
        s3.put_object(Bucket=RESULTS_BUCKET, Key=key, Body=(json.dumps(rec) + "\n").encode())
    except Exception:  # noqa: BLE001 — telemetry must never fail the request
        pass


def _exists(s3, key):
    try:
        s3.head_object(Bucket=RESULTS_BUCKET, Key=key)
        return True
    except Exception:
        return False


# ---- email receipt (ADR 0026 §3, default-on) ---------------------------------
CONTACT = "tpoongundranar@urban.org"
DOCS_URL = os.environ.get("DOCS_URL", "https://nccs.urban.org")   # confirm the canonical docs URL
CITATION = ("National Center for Charitable Statistics (NCCS), Urban Institute. "
            "Data accessed via the NCCS Data API.")


def _double_count_note(forms):
    """990combined already unions 990 + 990-EZ; flag if selected alongside them."""
    if "990combined" in forms and ({"990", "990ez"} & set(forms)):
        return ("Note: this export includes 990combined (which already unions 990 and "
                "990-EZ) alongside 990 and/or 990-EZ, so some organizations may appear "
                "more than once.")
    return ""


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
    caveat = _double_count_note(info["forms"])
    caveat_txt = f"\n{caveat}\n" if caveat else ""
    caveat_html = f'<p style="color:#b45309">{caveat}</p>' if caveat else ""
    text = ("Your NCCS data export is ready.\n\n"
            f"Download your data:\n{info['data_url']}\n\n"
            f"Download the data dictionary (explains each column):\n{info['dict_url']}\n\n"
            "Your export:\n" + "\n".join(f"  {k}: {v}" for k, v in summary) + "\n"
            f"{caveat_txt}\n"
            "These links stay valid: the files are kept for 30 days, and the links "
            "regenerate them on demand after that.\n\n"
            f"Column definitions & methodology: {DOCS_URL}\n"
            f"How to cite: {CITATION}\n\n"
            f"Questions? {CONTACT}")
    rows = "".join(f"<tr><td><b>{k}</b>&nbsp;</td><td>{v}</td></tr>" for k, v in summary)
    html = ("<p>Your NCCS data export is ready.</p>"
            f'<p><a href="{info["data_url"]}">Download your data</a></p>'
            f'<p><a href="{info["dict_url"]}">Download the data dictionary</a> (explains each column)</p>'
            f"<p><b>Your export</b></p><table>{rows}</table>"
            f"{caveat_html}"
            "<p>These links stay valid — the files are kept for 30 days and the links "
            "regenerate them on demand after that.</p>"
            f'<p>Column definitions &amp; methodology: <a href="{DOCS_URL}">{DOCS_URL}</a></p>'
            f"<p><b>How to cite:</b> {CITATION}</p>"
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
def _estimate(con, plan):
    """ADR 0026 §6 size pre-check: exact row_count + sampled byte estimate, no
    materialization / S3 write / email. The dashboard gates the export UX on this."""
    sql, params = _build_sql(plan["cols"], plan["filters"], plan["src"], plan["core_paths"])
    rows = con.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()[0]
    est_bytes = 0
    if rows:
        sample = "/tmp/est_sample.csv"
        con.execute(f"COPY (SELECT * FROM ({sql}) LIMIT 5000) TO '{sample}' (FORMAT csv, HEADER);", params)
        n = min(5000, rows)
        est_bytes = int(os.path.getsize(sample) / n * rows)
        os.remove(sample)
    return _resp(200, {"estimate": True, "row_count": rows,
                       "columns": len(plan["cols"]), "estimated_bytes": est_bytes})


def _create_export(event, s3):
    req = _parse_body(event)
    con = _con()
    plan = _plan(con, req)
    if req.get("estimate"):                  # size pre-check only — return early
        return _estimate(con, plan)
    job_id = str(uuid.uuid4())
    email = req.get("email")
    _log_event(s3, "request_created", {
        "job_id": job_id, "requester": _hash_requester(email),
        "tax_years": plan["years"], "forms": plan["forms"],
        "n_columns": len(plan["cols"]), "n_filters": len(plan["filters"]), "format": plan["fmt"]})
    t0 = time.monotonic()
    try:
        m = _materialize(con, plan, job_id, s3)
    except Exception:
        _log_event(s3, "export_materialized",
                   {"job_id": job_id, "success": False, "duration_ms": int((time.monotonic() - t0) * 1000)})
        raise
    _log_event(s3, "export_materialized",
               {"job_id": job_id, "success": True, "row_count": m["row_count"],
                "bytes": m["bytes"], "duration_ms": int((time.monotonic() - t0) * 1000)})
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
    rematerialized = not _exists(s3, target_key)
    if rematerialized:                               # swept by the 30-day lifecycle -> re-run
        con = _con()                                  # re-materialize regenerates BOTH result + dict
        plan = _plan(con, registry["request"])
        _materialize(con, plan, job_id, s3)
    _log_event(s3, "download", {"job_id": job_id, "kind": kind, "rematerialized": rematerialized})
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
