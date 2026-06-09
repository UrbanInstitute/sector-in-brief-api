"""
sector-in-brief-api — /data export handler (slice 1, ADR 0008 / 0026).

DuckDB-on-parquet: validate the request against the live parquet schema, join the
requested CORE form tiers to bmf-master-geocoded on EIN at query time (ADR 0016),
materialize the result to the results bucket (pattern B), assemble a data
dictionary merged from the per-dataset dictionaries (filtered to the delivered
columns), and return presigned URLs for both. Synchronous behind a Lambda
Function URL (no API Gateway 29s cap). Interface: ../openapi.yaml.

Security model: column *names* can't be SQL-parameterized, so every requested
column / filter key is whitelisted against the live schema before it touches the
SQL; filter *values* are bound parameters.
"""
import os, json, base64, uuid, boto3, duckdb

NCCS = "s3://nccsdata"
BMF = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded.parquet"
BMF_DICT = f"{NCCS}/geocoding/bmf-master/merged/bmf_master_geocoded_data_dictionary.csv"
RESULTS_BUCKET = os.environ["RESULTS_BUCKET"]
URL_TTL = int(os.environ.get("URL_TTL_SECONDS", "3600"))   # <= object lifetime (30d)
ALL_FORMS = ["990", "990ez", "990pf", "990combined"]
EXT = {"csv": "csv", "parquet": "parquet"}


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


# ---- request handling --------------------------------------------------------
def _parse(event):
    if isinstance(event, dict) and "body" in event:           # Lambda Function URL / API GW
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


def _presign(s3, key):
    return s3.generate_presigned_url("get_object",
                                     Params={"Bucket": RESULTS_BUCKET, "Key": key}, ExpiresIn=URL_TTL)


def _resp(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "body": json.dumps(body)}


def lambda_handler(event, context):
    try:
        req = _parse(event)
        con = _con()
        years = req.get("tax_years") or []
        forms = req.get("forms", ALL_FORMS)
        core_paths = _core_parquets(years, forms) if years else []
        if not core_paths:
            raise BadRequest("tax_years must be a non-empty list of years")
        src = _column_sources(con, core_paths)
        years, forms, cols, filters, fmt = _validate(req, src)

        job_id = str(uuid.uuid4())
        sql, params = _build_sql(cols, filters, src, core_paths)
        result_key = f"results/{job_id}.{EXT[fmt]}"
        rows = con.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()[0]
        con.execute(f"COPY ({sql}) TO 's3://{RESULTS_BUCKET}/{result_key}' (FORMAT {fmt}, HEADER);", params)

        dict_sql, dict_params = _dictionary_sql(cols, src, _core_dicts(years, forms))
        dict_key = f"results/{job_id}_dictionary.csv"
        con.execute(f"COPY ({dict_sql}) TO 's3://{RESULTS_BUCKET}/{dict_key}' (FORMAT csv, HEADER);", dict_params)

        s3 = boto3.client("s3", region_name="us-east-1")
        size = s3.head_object(Bucket=RESULTS_BUCKET, Key=result_key)["ContentLength"]
        return _resp(200, {
            "job_id": job_id,
            "row_count": rows,
            "result": {"format": fmt, "bytes": size, "url": _presign(s3, result_key),
                       "expires_in_seconds": URL_TTL},
            "data_dictionary": {"url": _presign(s3, dict_key), "columns": len(cols)},
        })
    except BadRequest as e:
        return _resp(400, {"error": "validation_error", "detail": str(e)})
    except Exception as e:  # noqa: BLE001 — surface the failure class to the caller
        return _resp(500, {"error": type(e).__name__, "detail": str(e)[:300]})
