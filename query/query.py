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
from botocore.exceptions import ClientError
from datetime import datetime, timezone

NCCS = "s3://nccsdata"
NCCS_BUCKET = "nccsdata"
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
# Dashboard-canonical NTEEv2 subsector labels (mirror of sector-in-brief-data
# R/table_builder_subsector.R). DELIBERATELY differs from the raw
# nteev2_subsector_definition already in bmf: EDU/HEL are "(minus Universities/
# Hospitals)" and UNU/unmapped/NULL fold into "Other" (the bmf column says
# "Education"/"Health"/"Unknown, Unclassified" and has NULLs) — so the API
# OVERRIDES the bmf column with this map. 11 named codes + the "Other" catch-all
# = the 12 labels the dashboard validates against.
SUBSECTOR_DEFINITION = {
    "ART": "Arts, Culture, and Humanities",
    "EDU": "Education (minus Universities)",
    "HEL": "Health (minus Hospitals)",
    "HMS": "Human Services",
    "IFA": "International, Foreign Affairs",
    "PSB": "Public, Societal Benefit",
    "REL": "Religion Related",
    "MMB": "Mutual/Membership Benefit",
    "UNI": "Universities",
    "HOS": "Hospitals",
    "ENV": "Environment and Animals",
}
SUBSECTOR_DEFINITION_OTHER = "Other"   # UNU + any code not in the table + NULL
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
    "org_type": ("IRS 501(c) subsection classification; 501(c)(3) split into Public Charities "
                 "vs Private Foundations (mirrors sector-in-brief-data derive_organization_type).",
                 "character"),
    "nteev2_subsector_definition": (
        "Plain-English NTEEv2 subsector label (mirrors sector-in-brief-data "
        "table_builder_subsector.R). OVERRIDES the raw bmf column of the same name: "
        "EDU is 'Education (minus Universities)', HEL 'Health (minus Hospitals)', and "
        "UNU/unmapped/NULL all fold into 'Other' (so it is null-free).", "character"),
}
RESULTS_BUCKET = os.environ["RESULTS_BUCKET"]
URL_TTL = int(os.environ.get("URL_TTL_SECONDS", "3600"))   # <= object lifetime (30d)
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")              # verified SES sender; receipt is sent if set
DOWNLOAD_BASE_URL = os.environ.get("DOWNLOAD_BASE_URL", "")  # public /download Function URL (for the email link)
DOWNLOAD_ONLY = bool(os.environ.get("DOWNLOAD_ONLY"))     # public download function: refuse /data
# ADR 0030: giant exports (> threshold) route to a Fargate worker instead of
# materializing synchronously on Lambda (10 GB join-memory cap). ECS_CLUSTER unset
# => async disabled (everything runs sync), so the API degrades safely pre-deploy.
ASYNC_THRESHOLD_BYTES = int(os.environ.get("ASYNC_THRESHOLD_BYTES", str(8 * 1024**3)))
ECS_CLUSTER = os.environ.get("ECS_CLUSTER", "")
ECS_TASK_DEF = os.environ.get("ECS_TASK_DEF", "")
ECS_CONTAINER = os.environ.get("ECS_CONTAINER", "worker")
ECS_SUBNETS = [s for s in os.environ.get("ECS_SUBNETS", "").split(",") if s]
ECS_SECURITY_GROUP = os.environ.get("ECS_SECURITY_GROUP", "")
ALL_FORMS = ["990", "990ez", "990pf", "990combined"]
# Per-form CORE tier. This API is canonical (the dashboard follows): 990combined
# and 990pf are served from the full-range within-core panel (core-panel,
# processed_merged/core/, 1987-2024); 990 and 990ez are separate forms that exist
# only in the modern tier (core-990, processed/core/, 2012+). 990combined here is
# therefore the panel's 990+990EZ union on the common variable set. See
# nccs-contracts ADR 0008/0016 + nccs-data-core core-{990,panel} contracts.
CORE_PREFIX = {
    "990combined": "processed_merged/core",
    "990pf":       "processed_merged/core",
    "990":         "processed/core",
    "990ez":       "processed/core",
}
EXT = {"csv": "csv", "parquet": "parquet"}
JOB_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class BadRequest(Exception):
    pass


# ---- path builders -----------------------------------------------------------
def _core_key(y, f):
    return f"{CORE_PREFIX[f]}/{y}/{f}/core_{y}_{f}.parquet"


def _core_dict_key(y, f):
    return f"{CORE_PREFIX[f]}/{y}/{f}/core_{y}_{f}_dictionary.csv"


def _core_parquets(years, forms):
    return [f"{NCCS}/{_core_key(y, f)}" for y in years for f in forms]


def _resolve_core_partitions(years, forms):
    """The (year, form) pairs to read, each confirmed present in its form's tier
    (CORE_PREFIX). Not every (year, form) exists — 990/990ez are modern-only
    (2012+), 990combined/990pf span 1987-2024 via the panel — so HEAD each key and
    FAIL LOUDLY listing any gaps. Never silently skip a missing partition: that
    would drop data from the export with no error (issue #14)."""
    s3 = boto3.client("s3")
    have, missing = [], []
    for f in forms:
        for y in years:
            try:
                s3.head_object(Bucket=NCCS_BUCKET, Key=_core_key(y, f))
                have.append((y, f))
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
                    raise            # a real access error is not a missing partition
                missing.append((y, f))
    if missing:
        raise BadRequest("no published core data for: "
                         + ", ".join(f"{f} {y}" for y, f in missing)
                         + " (990/990ez are 2012+; 990combined/990pf cover 1987-2024)")
    return have


def _core_dicts(pairs):
    """One representative dictionary CSV per form — descriptions are stable across
    years, so use the latest vintage for each form (routed to that form's tier)."""
    latest = {}
    for y, f in pairs:
        if y > latest.get(f, -1):
            latest[f] = y
    return [f"{NCCS}/{_core_dict_key(y, f)}" for f, y in latest.items()]


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


def _org_type_case():
    """Byte-for-byte mirror of sector-in-brief-data derive_organization_type(
    subsection_code, foundation_code) — incl. the 501(c)(3) PC/PF split and the
    Public-Charities catch-all default. (subsection_code/foundation_code are
    VARCHAR in bmf; foundation_code compared as string.)"""
    sc = "TRY_CAST(b.subsection_code AS INTEGER)"
    return (
        "CASE "
        f"WHEN {sc} IS NULL THEN '501(c)(3) Public Charities' "
        f"WHEN {sc} = 3 AND b.foundation_code IN ('2','3','4') THEN '501(c)(3) Private Foundations' "
        f"WHEN {sc} = 3 THEN '501(c)(3) Public Charities' "
        f"WHEN {sc} BETWEEN 1 AND 29 THEN '501(c)(' || {sc} || ')' "
        f"WHEN {sc} = 40 THEN '501(c)(d)' "
        f"WHEN {sc} = 50 THEN '501(c)(e)' "
        f"WHEN {sc} = 60 THEN '501(c)(f)' "
        f"WHEN {sc} = 70 THEN '501(c)(k)' "
        "ELSE '501(c)(3) Public Charities' END"
    )


def _subsector_def_case():
    """Map nteev2_subsector code -> the dashboard's canonical label
    (sector-in-brief-data R/table_builder_subsector.R). OVERRIDES the raw
    nteev2_subsector_definition already in bmf (whose EDU/HEL/UNU labels differ
    and which has NULLs); here UNU/unmapped/NULL all fold into 'Other', so the
    output is null-free and its distinct values are exactly the 12 labels."""
    whens = " ".join(f"WHEN {code!r} THEN {label!r}"
                     for code, label in SUBSECTOR_DEFINITION.items())
    return f"CASE b.nteev2_subsector {whens} ELSE {SUBSECTOR_DEFINITION_OTHER!r} END"


def _bmf_source():
    """bmf-master-geocoded enriched with the crosswalk-derived geo columns. Aliased `b`.
    Mirrors sector-in-brief-data/R/read_bmf.R: county-label join (ambiguous->NULL),
    CT override by %.2f coordinate, CBSA on the coalesced FIPS, region from state.
    REPLACE overrides bmf's raw nteev2_subsector_definition with the dashboard-
    canonical label (the column name pre-exists, so it's replaced, not appended)."""
    return f"""(
      SELECT b.* REPLACE ({_subsector_def_case()} AS nteev2_subsector_definition),
        COALESCE(cf.geo_county_fips, ct.geo_county_fips)             AS geo_county_fips,
        COALESCE(cf.geo_county_canonical, ct.geo_county_canonical)   AS geo_county_canonical,
        cb.cbsa_code, cb.cbsa_title, cb.cbsa_type, cb.csa_code, cb.csa_title,
        {_region_case()}                                            AS census_region,
        {_org_type_case()}                                          AS org_type
      FROM read_parquet('{BMF}') b
      LEFT JOIN read_parquet('{CXW_COUNTY}') cf
        ON b.geo_state_abbr = cf.geo_state_abbr AND b.geo_county = cf.geo_county_raw
      -- CT override applies only to CT-state rows. The 'CT' literal is carried on
      -- the ct side so `b.geo_state_abbr = ct._ct_state` is a real two-sided equi-
      -- join KEY, not a single-sided filter. Otherwise DuckDB hash-joins on the
      -- rounded coords alone and probes all ~3.7M rows: non-CT orgs sharing a 2dp
      -- coord cell with CT points fan out into the billions before the state filter
      -- prunes them (unfiltered full-registry hangs). With state in the key, non-CT
      -- rows miss the hash immediately. Same result, no fan-out.
      LEFT JOIN (SELECT *, 'CT' AS _ct_state FROM read_parquet('{CXW_CT}')) ct
        ON b.geo_state_abbr = ct._ct_state
        AND printf('%.2f', b.geo_lat) = printf('%.2f', ct.lat2)
        AND printf('%.2f', b.geo_lon) = printf('%.2f', ct.lon2)
      LEFT JOIN read_parquet('{CXW_CBSA}') cb
        ON COALESCE(cf.geo_county_fips, ct.geo_county_fips) = cb.county_fips
    )"""


def _column_sources(con, core_paths):
    """Map every queryable column to its alias: 'c' (core) wins overlaps, else 'b'
    (enriched bmf, incl. the crosswalk-derived geo columns). With no core_paths
    (source='bmf' — the org-level registry alone) the map is bmf-only."""
    bmf_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {_bmf_source()} b").fetchall()]
    src = {c: "b" for c in bmf_cols}
    if core_paths:
        core_cols = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({_sql_list(core_paths)}, union_by_name=true)").fetchall()]
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


def _validate(req, src, mode="core"):
    if mode == "core":
        years = req.get("tax_years")
        if not isinstance(years, list) or not years:
            raise BadRequest("tax_years must be a non-empty list of years")
        forms = req.get("forms", ALL_FORMS)
        if not all(f in ALL_FORMS for f in forms):
            raise BadRequest(f"forms must be a subset of {ALL_FORMS}")
    else:                              # source='bmf' — org-level registry, no tax-year partition / forms
        years, forms = [], []
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
    # An explicitly empty filter list is malformed: "no filter on this column" is
    # expressed by OMITTING the key (which match-alls), so an empty list only
    # happens when a caller's selection broke. Reject it loudly rather than
    # silently exporting the whole dataset (issue #13). Mandatory-vs-optional is a
    # consumer (dashboard) concern; the API just refuses any value-less filter.
    empty = [k for k, v in filters.items() if isinstance(v, list) and not v]
    if empty:
        raise BadRequest(
            f"filter(s) with no values: {sorted(empty)}; omit the key to leave a column unfiltered")
    return years, forms, cols, filters, fmt


def _build_sql(cols, filters, src, core_paths, active_years=None):
    select = ", ".join(f'{src[c]}."{c}" AS "{c}"' for c in cols)
    clauses, params = [], []
    for k, vals in (filters or {}).items():
        vals = vals if isinstance(vals, list) else [vals]
        if not vals:                 # _validate rejects empty filter lists up front;
            continue                 # this is last-resort defense so we never emit `IN ()`.
        clauses.append(f'{src[k]}."{k}" IN ({", ".join(["?"] * len(vals))})')
        params += [str(v) for v in vals]
    if active_years:   # BMF year filter: org lifespan [first,last] OVERLAPS [min,max] of the span (see _plan)
        clauses.append('b."first_year_in_bmf" <= ? AND b."last_year_in_bmf" >= ?')
        params += [max(active_years), min(active_years)]
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    if core_paths:
        sql = (f"SELECT {select} "
               f"FROM read_parquet({_sql_list(core_paths)}, union_by_name=true) c "
               f'JOIN {_bmf_source()} b ON c."ein" = b."ein" {where}')
    else:              # source='bmf': the org-level registry alone, no core join
        sql = f"SELECT {select} FROM {_bmf_source()} b {where}"
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
        # These are computed at query time, so there's no precomputed source null_pct.
        # NULL_FREE columns are null-free by construction (their CASE has an ELSE
        # catch-all) -> stated as 0. The rest (county/CBSA/region join outputs) DO
        # carry meaningful nulls; leave null_pct NULL but flag it in the description
        # so a blank reads as "not computed", not "0%" (issue #15).
        NULL_FREE = {"org_type", "nteev2_subsector_definition"}
        rows = []
        for c in derived:
            desc = DERIVED_COLUMNS[c][0]
            null_pct = "0.0" if c in NULL_FREE else "CAST(NULL AS DOUBLE)"
            if c not in NULL_FREE:
                desc += " (crosswalk-derived; null rate not precomputed)"
            rows.append("('{c}', 'crosswalk', '{d}', '{t}', {n})".format(
                c=c, d=desc.replace("'", "''"), t=DERIVED_COLUMNS[c][1], n=null_pct))
        vals = ", ".join(rows)
        parts.append('SELECT "column", source, description, data_type, null_pct '
                     f'FROM (VALUES {vals}) AS t("column", source, description, data_type, null_pct)')
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
    if "geo_state_abbr" in out and out["geo_state_abbr"]:
        states &= set(out["geo_state_abbr"])
        if not states:   # contradictory region+state filters: return a clear 400, not
            raise BadRequest(  # a silent match-all (empty geo_state_abbr -> dropped predicate).
                "census_region and geo_state_abbr filters don't overlap "
                f"({sorted(filters['census_region'])} vs {sorted(out['geo_state_abbr'])})")
    out["geo_state_abbr"] = sorted(states)
    return out


def _validate_active_years(req):
    """BMF year filtering (source='bmf'). The org-level registry has no per-year
    partition; each row carries a lifespan [first_year_in_bmf, last_year_in_bmf].
    `active_years` selects orgs whose lifespan OVERLAPS the requested span — NOT an
    `IN` on a single year column (which would mean "org's first/last year == Y", a
    ~19x-smaller, wrong set). `tax_years` is a CORE partition concept and is rejected
    here so the two never get silently conflated. Returns the list, or None."""
    if "tax_years" in req:
        raise BadRequest("tax_years applies to source='core'; use active_years for BMF lifespan filtering")
    ys = req.get("active_years")
    if ys is None:
        return None
    if not isinstance(ys, list) or not ys or not all(isinstance(y, int) for y in ys):
        raise BadRequest("active_years must be a non-empty list of integer years")
    return ys


BMF_LIFESPAN_COLS = ("first_year_in_bmf", "last_year_in_bmf")


def _with_provenance_cols(cols, active_years):
    """When an active_years overlap filter is applied, force BOTH lifespan columns
    into the output so the predicate is self-auditing from the result alone (record
    evidence, not verdicts). Both are needed: an overlap match can't be verified from
    one endpoint. Deduped, appended after the caller's columns (ein is already first)."""
    if not active_years:
        return cols
    return cols + [c for c in BMF_LIFESPAN_COLS if c not in cols]


def _plan(con, req):
    mode = req.get("source", "core")
    if mode not in ("core", "bmf"):
        raise BadRequest("source must be 'core' or 'bmf'")
    if mode == "core":
        years = req.get("tax_years") or []
        forms = req.get("forms", ALL_FORMS)
        if not isinstance(years, list) or not years:
            raise BadRequest("tax_years must be a non-empty list of years")
        if not all(f in ALL_FORMS for f in forms):
            raise BadRequest(f"forms must be a subset of {ALL_FORMS}")
        core_pairs = _resolve_core_partitions(years, forms)   # raises on any missing (year, form)
        core_paths = [f"{NCCS}/{_core_key(y, f)}" for y, f in core_pairs]
        active_years = None
    else:                              # source='bmf': org-level registry, no core join
        core_pairs, core_paths, active_years = [], [], _validate_active_years(req)
    src = _column_sources(con, core_paths)
    years, forms, cols, filters, fmt = _validate(req, src, mode)
    cols = _with_provenance_cols(cols, active_years)
    filters = _expand_region_filter(filters)   # region -> states, so it pushes down (slice 5.1)
    return dict(mode=mode, years=years, forms=forms, cols=cols, filters=filters, fmt=fmt,
                src=src, core_paths=core_paths, core_pairs=core_pairs, active_years=active_years)


def _materialize(con, plan, job_id, s3):
    sql, params = _build_sql(plan["cols"], plan["filters"], plan["src"], plan["core_paths"], plan["active_years"])
    fmt = plan["fmt"]
    result_key = f"results/{job_id}.{EXT[fmt]}"
    # HEADER is a CSV-only option; parquet rejects it (NotImplementedException). The
    # COPY itself returns the rows-written count, so no separate count(*) pass — that
    # would re-run the whole join (a wasteful 2x on every export).
    copy_opts = "(FORMAT csv, HEADER)" if fmt == "csv" else "(FORMAT parquet)"
    timings = {}
    t = time.monotonic()
    rows = con.execute(
        f"COPY ({sql}) TO 's3://{RESULTS_BUCKET}/{result_key}' {copy_opts};", params).fetchone()[0]
    timings["result_copy_ms"] = int((time.monotonic() - t) * 1000)
    t = time.monotonic()
    core_dicts = _core_dicts(plan["core_pairs"]) if plan["core_pairs"] else []
    dsql, dparams = _dictionary_sql(plan["cols"], plan["src"], core_dicts)
    dict_key = f"results/{job_id}_dictionary.csv"
    con.execute(f"COPY ({dsql}) TO 's3://{RESULTS_BUCKET}/{dict_key}' (FORMAT csv, HEADER);", dparams)
    timings["dictionary_copy_ms"] = int((time.monotonic() - t) * 1000)
    size = s3.head_object(Bucket=RESULTS_BUCKET, Key=result_key)["ContentLength"]
    return dict(row_count=rows, result_key=result_key, dict_key=dict_key, bytes=size,
                columns=len(plan["cols"]), format=fmt, timings=timings)


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
    if info.get("source") == "bmf":
        scope = [("Source", "BMF (organization registry)"),
                 ("Active years", ", ".join(map(str, info.get("active_years") or [])) or "all")]
    else:
        scope = [("Tax years", ", ".join(map(str, info["tax_years"]))),
                 ("Form types", ", ".join(info["forms"]))]
    summary = [("Rows", f"{info['row_count']:,}"), ("Format", info["format"].upper()), *scope,
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
def _estimate_size(con, plan):
    """Exact row_count + sampled byte estimate (count + 5k-row sample), no S3
    write / email. Shared by the /data estimate pre-check and the ADR 0030 async
    router. Returns (rows, est_bytes)."""
    sql, params = _build_sql(plan["cols"], plan["filters"], plan["src"], plan["core_paths"], plan["active_years"])
    rows = con.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()[0]
    est_bytes = 0
    if rows:
        sample = "/tmp/est_sample.csv"
        con.execute(f"COPY (SELECT * FROM ({sql}) LIMIT 5000) TO '{sample}' (FORMAT csv, HEADER);", params)
        n = min(5000, rows)
        est_bytes = int(os.path.getsize(sample) / n * rows)
        os.remove(sample)
    return rows, est_bytes


def _estimate(con, plan):
    """ADR 0026 §6 size pre-check: the dashboard gates the export UX on this."""
    rows, est_bytes = _estimate_size(con, plan)
    return _resp(200, {"estimate": True, "row_count": rows,
                       "columns": len(plan["cols"]), "estimated_bytes": est_bytes})


def _needs_estimate_for_routing(filters):
    """ADR 0030 routing gate (pure). A state/region-bounded request is safely far
    under the async threshold (largest: CA all-years/forms ~16 MB), so it skips the
    estimate and stays on the fast sync path. Only geographically-broad requests —
    the only ones that can reach the 30-51 GB giant tail — pay a count to be sized.
    (census_region is expanded to geo_state_abbr in _plan, so it counts as bounded.)"""
    return "geo_state_abbr" not in (filters or {})


def _registry(plan, job_id, email, status, result_key=None):
    """The requests/{job_id}.json sidecar (ADR 0026 §2). `status` (ADR 0030):
    'ready' for a sync export, 'pending'->'ready'/'failed' for an async one."""
    return {
        "job_id": job_id,
        "request": {"source": plan["mode"], "tax_years": plan["years"], "forms": plan["forms"],
                    "active_years": plan["active_years"],
                    "columns": plan["cols"], "filters": plan["filters"], "format": plan["fmt"]},
        "email": email,
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "result_key": result_key,
    }


def _durable_urls(job_id):
    base = DOWNLOAD_BASE_URL.rstrip("/")
    data_url = f"{base}/download/{job_id}" if base else f"/download/{job_id}"
    return base, data_url, f"{data_url}?kind=dictionary"


def _send_receipt_for(plan, job_id, email, m):
    """Default-on receipt (ADR 0026 §3). Sent by the request on the sync path and
    by the worker on the async path — same durable link either way."""
    if not email:
        return None
    _, data_url, dict_url = _durable_urls(job_id)
    return _send_receipt(email, {
        "data_url": data_url, "dict_url": dict_url, "row_count": m["row_count"],
        "format": m["format"], "source": plan["mode"], "tax_years": plan["years"],
        "forms": plan["forms"], "active_years": plan["active_years"],
        "columns": plan["cols"], "filters": plan["filters"], "job_id": job_id})


def _create_export(event, s3):
    req = _parse_body(event)
    t_con = time.monotonic()
    con = _con()                             # cold start: httpfs install/load + S3 secret
    connect_ms = int((time.monotonic() - t_con) * 1000)
    plan = _plan(con, req)
    if req.get("estimate"):                  # size pre-check only — return early
        return _estimate(con, plan)
    job_id = str(uuid.uuid4())
    email = req.get("email")
    _log_event(s3, "request_created", {
        "job_id": job_id, "requester": _hash_requester(email),
        "source": plan["mode"], "tax_years": plan["years"], "forms": plan["forms"],
        "active_years": plan["active_years"],
        "n_columns": len(plan["cols"]), "n_filters": len(plan["filters"]), "format": plan["fmt"]})
    # ADR 0030: size a geographically-broad request and route a giant (> threshold,
    # over Lambda's 10 GB cap) to the Fargate worker. State/region-bounded requests
    # skip the estimate and stay synchronous.
    if ECS_CLUSTER and _needs_estimate_for_routing(plan["filters"]):
        _, est_bytes = _estimate_size(con, plan)
        if est_bytes > ASYNC_THRESHOLD_BYTES:
            return _dispatch_async(plan, job_id, email, est_bytes, s3)
    t0 = time.monotonic()
    try:
        m = _materialize(con, plan, job_id, s3)
    except Exception:
        _log_event(s3, "export_materialized",
                   {"job_id": job_id, "success": False, "duration_ms": int((time.monotonic() - t0) * 1000)})
        raise
    timings = {"connect_ms": connect_ms, **m["timings"],
               "total_materialize_ms": int((time.monotonic() - t0) * 1000)}
    print(json.dumps({"phase_timings": timings, "job_id": job_id,
                      "row_count": m["row_count"], "bytes": m["bytes"], "format": m["format"]}))
    _log_event(s3, "export_materialized",
               {"job_id": job_id, "success": True, "row_count": m["row_count"],
                "bytes": m["bytes"], "duration_ms": timings["total_materialize_ms"],
                "timings": timings})
    s3.put_object(Bucket=RESULTS_BUCKET, Key=f"requests/{job_id}.json",
                  Body=json.dumps(_registry(plan, job_id, email, "ready", m["result_key"])),
                  ContentType="application/json")
    base, data_url, dict_url = _durable_urls(job_id)
    receipt = _send_receipt_for(plan, job_id, email, m)
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


def _dispatch_async(plan, job_id, email, est_bytes, s3):
    """ADR 0030: persist a pending registry, launch a one-shot Fargate task to
    materialize the giant, and return 202 immediately. The worker (run_async_job)
    flips the registry to ready and sends the receipt when it finishes."""
    s3.put_object(Bucket=RESULTS_BUCKET, Key=f"requests/{job_id}.json",
                  Body=json.dumps(_registry(plan, job_id, email, "pending")),
                  ContentType="application/json")
    boto3.client("ecs", region_name="us-east-1").run_task(
        cluster=ECS_CLUSTER, taskDefinition=ECS_TASK_DEF, launchType="FARGATE", count=1,
        networkConfiguration={"awsvpcConfiguration": {
            "subnets": ECS_SUBNETS, "securityGroups": [ECS_SECURITY_GROUP],
            "assignPublicIp": "ENABLED"}},   # default-VPC public subnets: need a public IP to reach S3/ECR/SES
        overrides={"containerOverrides": [{"name": ECS_CONTAINER,
                                           "environment": [{"name": "JOB_ID", "value": job_id}]}]})
    _log_event(s3, "export_dispatched_async", {"job_id": job_id, "estimated_bytes": est_bytes})
    base, data_url, dict_url = _durable_urls(job_id)
    return _resp(202, {
        "job_id": job_id, "status": "pending", "estimated_bytes": est_bytes,
        "message": ("Large export started; a download link will be emailed when ready."
                    if email else "Large export started; poll download_path for status."),
        "download_path": f"/download/{job_id}",
        "download_url": data_url if base else None,
        "dictionary_download_url": dict_url if base else None,
        "email": {"to": email, "status": "pending"} if email else None,
    })


def run_async_job(job_id, s3):
    """Fargate worker entrypoint (ADR 0030). Materialize a pending giant job, flip
    the registry to ready/failed, and send the email receipt — reusing the sync
    path's _materialize so there is exactly one materialization implementation."""
    key = f"requests/{job_id}.json"
    reg = json.loads(s3.get_object(Bucket=RESULTS_BUCKET, Key=key)["Body"].read())
    con = _con()
    plan = _plan(con, reg["request"])
    t0 = time.monotonic()
    try:
        m = _materialize(con, plan, job_id, s3)
    except Exception as e:  # noqa: BLE001
        reg["status"], reg["error"] = "failed", str(e)[:300]
        s3.put_object(Bucket=RESULTS_BUCKET, Key=key, Body=json.dumps(reg), ContentType="application/json")
        _log_event(s3, "export_materialized", {"job_id": job_id, "success": False, "async": True,
                                               "duration_ms": int((time.monotonic() - t0) * 1000)})
        raise
    reg["status"], reg["result_key"] = "ready", m["result_key"]
    s3.put_object(Bucket=RESULTS_BUCKET, Key=key, Body=json.dumps(reg), ContentType="application/json")
    timings = {**m["timings"], "total_materialize_ms": int((time.monotonic() - t0) * 1000)}
    print(json.dumps({"phase_timings": timings, "job_id": job_id, "async": True,
                      "row_count": m["row_count"], "bytes": m["bytes"], "format": m["format"]}))
    _log_event(s3, "export_materialized",
               {"job_id": job_id, "success": True, "async": True, "row_count": m["row_count"],
                "bytes": m["bytes"], "duration_ms": timings["total_materialize_ms"], "timings": timings})
    _send_receipt_for(plan, job_id, reg.get("email"), m)


def _download(job_id, s3, kind="result"):
    if not JOB_RE.match(job_id or ""):
        return _resp(404, {"error": "unknown_job"})
    try:
        registry = json.loads(s3.get_object(Bucket=RESULTS_BUCKET, Key=f"requests/{job_id}.json")["Body"].read())
    except Exception:
        return _resp(404, {"error": "unknown_job"})
    if registry.get("status") == "pending":          # ADR 0030: async giant still running
        return _resp(202, {"job_id": job_id, "status": "pending",
                           "detail": "export is still being prepared; try again shortly"})
    if registry.get("status") == "failed":
        return _resp(500, {"job_id": job_id, "status": "failed",
                           "detail": registry.get("error", "export failed")})
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
