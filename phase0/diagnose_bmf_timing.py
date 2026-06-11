"""
Phase-0 diagnostic (ADR 0029): localize WHERE BMF-mode time goes.

measure_bmf_timing.py `geo` ran for minutes in-region — far past the ~5s gate-3
baseline (a BIGGER CORE join). That's pathological, not "heavy", so decompose:
time each stage of the query independently, cheapest-decisive first, so ONE run
says whether the cost is the parquet SCAN or the crosswalk JOINS.

Each stage runs `count(*)` (forces full execution, ~no output cost) and prints
seconds. The key comparison:

  [narrow scan]  read ONLY the ~8 columns the joins/derivations need.
  [b.* scan]     read `b.*` like production _bmf_source does.
      narrow fast + b.* slow  => the bug is `b.*` (no projection pushdown):
                                 _bmf_source reads the whole wide parquet.
  [+ county] [+ ct] [+ cbsa]  add one crosswalk join at a time.
      a single step jumps     => that join is the cost (CT printf is prime suspect).
  [full _bmf_source]          the production subquery, for the total.

Run in-region (us-east-1):  AWS_PROFILE=thiya python3 phase0/diagnose_bmf_timing.py
"""
import sys, os, time, json

os.environ.setdefault("RESULTS_BUCKET", "unused-local-measurement")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from query.query import _con, _bmf_source, BMF, CXW_COUNTY, CXW_CT, CXW_CBSA

# columns the joins + CASE derivations actually reference (what a pruned scan needs)
NEEDED = ["ein", "geo_state_abbr", "geo_county", "geo_lat", "geo_lon",
          "subsection_code", "foundation_code", "nteev2_subsector"]


def timed(con, label, sql):
    t0 = time.time()
    n = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]
    dt = time.time() - t0
    print(json.dumps({"stage": label, "rows": n, "seconds": round(dt, 2)}))
    return dt


def main():
    con = _con()

    timed(con, "narrow scan (8 cols)",
          f"SELECT {', '.join(NEEDED)} FROM read_parquet('{BMF}')")
    timed(con, "b.* scan (like _bmf_source)",
          f"SELECT b.* FROM read_parquet('{BMF}') b")

    base = f"SELECT {', '.join('b.'+c for c in NEEDED)} FROM read_parquet('{BMF}') b"
    timed(con, "+ county join",
          f"{base} LEFT JOIN read_parquet('{CXW_COUNTY}') cf "
          f"ON b.geo_state_abbr = cf.geo_state_abbr AND b.geo_county = cf.geo_county_raw")
    # OLD single-sided filter (b.geo_state_abbr = 'CT') — fans out, HANGS. Kept
    # for reference; do not enable on the full registry.
    timed(con, "+ ct join OLD (single-sided 'CT' filter)",
          f"{base} LEFT JOIN read_parquet('{CXW_CT}') ct "
          f"ON b.geo_state_abbr = 'CT' "
          f"AND printf('%.2f', b.geo_lat) = printf('%.2f', ct.lat2) "
          f"AND printf('%.2f', b.geo_lon) = printf('%.2f', ct.lon2)"
          ) if os.environ.get("RUN_OLD_CT") else print('{"stage": "+ ct join OLD", "skipped": "set RUN_OLD_CT=1 to run (it hangs)"}')
    # FIXED: 'CT' carried on the ct side -> real two-sided equi-join key.
    timed(con, "+ ct join FIXED (state-keyed)",
          f"{base} LEFT JOIN (SELECT *, 'CT' AS _ct_state FROM read_parquet('{CXW_CT}')) ct "
          f"ON b.geo_state_abbr = ct._ct_state "
          f"AND printf('%.2f', b.geo_lat) = printf('%.2f', ct.lat2) "
          f"AND printf('%.2f', b.geo_lon) = printf('%.2f', ct.lon2)")
    timed(con, "+ cbsa join",
          f"{base} LEFT JOIN read_parquet('{CXW_COUNTY}') cf "
          f"ON b.geo_state_abbr = cf.geo_state_abbr AND b.geo_county = cf.geo_county_raw "
          f"LEFT JOIN read_parquet('{CXW_CBSA}') cb ON cf.geo_county_fips = cb.county_fips")

    timed(con, "full _bmf_source() [production]", f"SELECT ein FROM {_bmf_source()} b")


if __name__ == "__main__":
    main()
