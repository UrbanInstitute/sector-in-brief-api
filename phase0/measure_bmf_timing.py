"""
Phase-0 measurement (ADR 0029): in-region worst-case timing + peak memory for
the BMF org-level query mode (source=bmf).

WHY this exists: dev-box (WSL2) timing over S3-from-internet is INVALID for the
host decision — gate 3 showed the same join was ~70x slower from outside AWS
(egress, not compute). So this must be run IN-REGION (operator on an EC2 box in
us-east-1, to keep control-plane calls off the assistant session). It mirrors
gate 3's methodology exactly: it reuses the PRODUCTION SQL builders from
query.query (so the measured SQL is byte-for-byte what ships, incl. the three
crosswalk LEFT JOINs + CASE derivations in _bmf_source) and writes the result to
local disk to isolate read+join+materialize cost from the S3 PutObject (the EC2
ec2-s3FullAccess role can read nccsdata but is denied PutObject on the results
bucket; the in-region upload is fast and not the bottleneck — see FINDINGS gate 3).

WORST CASE for BMF mode = NO filter (the whole 3.67M-row registry). active_years
only ever shrinks the set, so unfiltered is the upper bound. Two scenarios:

  wide  all BMF columns + the derived geo/classification columns, no filter.
        Worst-case OUTPUT SIZE and triggers every crosswalk join + CASE.
  geo   ein + only the derived geo columns (geo_county_fips, cbsa_code,
        census_region, org_type, nteev2_subsector_definition), no filter.
        Isolates the genuinely-unmeasured RISK: the county/ct/cbsa crosswalk
        joins running over the FULL registry (gate 3 only joined CORE filings).
  filt  same as `geo` but WITH active_years overlap, to see the filter's effect.

One scenario per process invocation, so resource.ru_maxrss is THAT scenario's
true peak RSS (the open question from FINDINGS gate 3 #2: peak join memory vs
Lambda's 10 GB cap). RSS is reported unconstrained on purpose — if it fits
unconstrained it fits in Lambda; a memory_limit would only mask it by spilling.

Run (in-region, us-east-1):
  AWS_PROFILE=thiya python phase0/measure_bmf_timing.py wide
  AWS_PROFILE=thiya python phase0/measure_bmf_timing.py geo
  AWS_PROFILE=thiya python phase0/measure_bmf_timing.py filt 2015 2018

Optional: prepend `DUCKDB_MEM=10GB` to simulate Lambda's max memory (forces spill
to /tmp above the cap instead of using all of the box's RAM).
"""
import sys, os, time, json, resource, tempfile

# Reuse the deployed handler's SQL generation so the measurement can't drift from
# what ships. _con() sets temp_directory=/tmp/duckdb_spill (the spill path).
# query.query reads RESULTS_BUCKET at import; we write locally, so it's unused —
# stub it so the import succeeds without a real bucket.
os.environ.setdefault("RESULTS_BUCKET", "unused-local-measurement")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from query.query import _con, _column_sources, _with_provenance_cols, _build_sql

DERIVED_GEO = ["geo_county_fips", "cbsa_code", "census_region", "org_type",
               "nteev2_subsector_definition"]


def main():
    scenario = sys.argv[1] if len(sys.argv) > 1 else "geo"
    active_years = [int(a) for a in sys.argv[2:]] or None

    con = _con()
    mem = os.environ.get("DUCKDB_MEM")
    if mem:
        con.execute(f"SET memory_limit='{mem}';")
    con.execute("PRAGMA enable_profiling='no_output';")

    src = _column_sources(con, [])          # core_paths=[] -> bmf-only column map
    if scenario == "wide":
        cols = list(src.keys())             # every native + derived column = worst output
        active_years = None
    elif scenario == "geo":
        cols = ["ein"] + DERIVED_GEO
        active_years = None
    elif scenario == "filt":
        cols = ["ein"] + DERIVED_GEO
        if not active_years:
            active_years = [2015, 2018]
    else:
        sys.exit(f"unknown scenario {scenario!r}; use wide|geo|filt")

    cols = _with_provenance_cols(cols, active_years)
    sql, params = _build_sql(cols, {}, src, [], active_years)

    t0 = time.time()
    n = con.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()[0]
    t_count = time.time() - t0

    out = os.path.join(tempfile.gettempdir(), f"bmf_{scenario}.csv")
    t0 = time.time()
    con.execute(f"COPY ({sql}) TO '{out}' (FORMAT csv, HEADER);", params)
    t_copy = time.time() - t0

    size = os.path.getsize(out)
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB->MB on Linux
    os.remove(out)

    print(json.dumps({
        "scenario": scenario,
        "active_years": active_years,
        "n_columns": len(cols),
        "rows": n,
        "result_bytes": size,
        "result_mb": round(size / 1024 / 1024, 1),
        "count_s": round(t_count, 2),
        "copy_s": round(t_copy, 2),
        "wall_s": round(t_count + t_copy, 2),
        "peak_rss_mb": round(rss_mb, 1),
        "duckdb_mem_limit": mem or "unlimited",
    }, indent=2))


if __name__ == "__main__":
    main()
