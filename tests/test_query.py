"""Unit tests for the new /data handler's pure logic (no AWS / DuckDB calls)."""
import os
os.environ.setdefault("RESULTS_BUCKET", "test-bucket")  # query.py reads this at import

import pytest
from query.query import (_validate, _build_sql, _dictionary_sql, _core_parquets,
                         _sql_list, _double_count_note, _region_case, _expand_region_filter,
                         _org_type_case, _subsector_def_case, SUBSECTOR_DEFINITION,
                         _validate_active_years, _with_provenance_cols,
                         DERIVED_COLUMNS, CENSUS_REGION, BadRequest)

# fake column->source map (core wins overlaps): ein/total_revenue core; the rest bmf
SRC = {"ein": "c", "total_revenue": "c",
       "geo_state_abbr": "b", "org_name_display": "b", "ntee_common_code": "b"}
# BMF-only mode: every column resolves to the enriched bmf source ('b')
BMF_SRC = {"ein": "b", "geo_state_abbr": "b", "first_year_in_bmf": "b", "last_year_in_bmf": "b"}


def test_validate_ok_and_autoadds_ein():
    years, forms, cols, filters, fmt = _validate(
        {"tax_years": [2019], "columns": ["total_revenue"], "filters": {"geo_state_abbr": ["CA"]}}, SRC)
    assert years == [2019] and fmt == "csv"
    assert cols[0] == "ein"                      # ein is force-included as row identity
    assert forms == ["990", "990ez", "990pf", "990combined"]


def test_validate_rejects_unknown_column():
    with pytest.raises(BadRequest):
        _validate({"tax_years": [2019], "columns": ["not_a_column"]}, SRC)


def test_validate_rejects_unknown_filter_key():
    with pytest.raises(BadRequest):
        _validate({"tax_years": [2019], "columns": ["ein"], "filters": {"bogus": ["x"]}}, SRC)


def test_validate_requires_tax_years_and_columns():
    with pytest.raises(BadRequest):
        _validate({"columns": ["ein"]}, SRC)
    with pytest.raises(BadRequest):
        _validate({"tax_years": [2019], "columns": []}, SRC)


def test_build_sql_qualifies_columns_and_binds_filter_values():
    sql, params = _build_sql(["ein", "geo_state_abbr"],
                             {"geo_state_abbr": ["CA", "NY"]}, SRC, ["s3://b/x.parquet"])
    assert 'c."ein" AS "ein"' in sql
    assert 'b."geo_state_abbr" IN (?, ?)' in sql          # values parameterized, not interpolated
    assert 'c."ein" = b."ein"' in sql                     # EIN join
    assert "union_by_name=true" in sql
    assert params == ["CA", "NY"]


def test_dictionary_sql_splits_by_source():
    sql, params = _dictionary_sql(["ein", "geo_state_abbr"], SRC, ["s3://b/d.csv"])
    assert "'core' AS source" in sql and "'bmf-geocoded' AS source" in sql
    assert "ein" in params and "geo_state_abbr" in params


def test_core_parquets_is_years_x_forms():
    paths = _core_parquets([2019, 2020], ["990", "990ez"])
    assert len(paths) == 4
    assert paths[0].endswith("processed/core/2019/990/core_2019_990.parquet")


def test_sql_list_quotes_paths():
    assert _sql_list(["a", "b"]) == "['a', 'b']"


def test_double_count_note_only_when_combined_overlaps():
    assert _double_count_note(["990combined", "990"])        # overlap -> warn
    assert _double_count_note(["990", "990ez", "990combined"])
    assert not _double_count_note(["990combined"])           # combined alone -> fine
    assert not _double_count_note(["990", "990ez"])          # no combined -> fine
    assert not _double_count_note(["990pf", "990combined"])  # pf doesn't overlap combined


def test_census_region_covers_50_states_plus_dc_uniquely():
    allstates = [s for states in CENSUS_REGION.values() for s in states]
    assert len(allstates) == len(set(allstates))   # no state in two regions
    assert len(allstates) == 51                      # 50 states + DC
    for st, reg in {"CT": "Northeast", "CA": "West", "TX": "South",
                    "IL": "Midwest", "NY": "Northeast"}.items():
        assert st in CENSUS_REGION[reg]


def test_region_case_sql_shape():
    sql = _region_case()
    assert sql.startswith("CASE") and "'Northeast'" in sql and "b.geo_state_abbr IN" in sql


def test_dictionary_sql_emits_crosswalk_part_for_derived_columns():
    sql, params = _dictionary_sql(["geo_county_fips", "census_region"],
                                  {"geo_county_fips": "b", "census_region": "b"}, ["s3://d.csv"])
    assert "'crosswalk'" in sql and "geo_county_fips" in sql
    assert params == []         # derived entries are literals, not bound params / dict lookups


def test_expand_region_filter_to_states():
    f = _expand_region_filter({"census_region": ["Northeast"]})
    assert "census_region" not in f                              # derived filter is gone
    assert set(f["geo_state_abbr"]) == set(CENSUS_REGION["Northeast"])  # -> pushdown-able states


def test_expand_region_intersects_with_explicit_state_filter():
    f = _expand_region_filter({"census_region": ["West"], "geo_state_abbr": ["CA", "NY"]})
    assert f["geo_state_abbr"] == ["CA"]        # NY is Northeast -> AND drops it


def test_expand_region_passthrough_and_unknown():
    assert _expand_region_filter({"geo_state_abbr": ["RI"]}) == {"geo_state_abbr": ["RI"]}
    with pytest.raises(BadRequest):
        _expand_region_filter({"census_region": ["Westt"]})


def test_org_type_is_derived_and_mirrors_producer_case():
    assert "org_type" in DERIVED_COLUMNS                      # selectable + in the dictionary
    sql = _org_type_case()
    # the PC/PF split, the c(3) order (before the generic range), and the catch-all default
    assert "'501(c)(3) Private Foundations'" in sql and "'501(c)(3) Public Charities'" in sql
    assert "b.foundation_code IN ('2','3','4')" in sql
    assert sql.index("= 3 AND") < sql.index("BETWEEN 1 AND 29")   # c(3) handled before the range
    assert "'501(c)(d)'" in sql and "'501(c)(k)'" in sql
    assert sql.rstrip().endswith("ELSE '501(c)(3) Public Charities' END")


def test_subsector_definition_is_derived_and_canonical():
    assert "nteev2_subsector_definition" in DERIVED_COLUMNS     # selectable + in the dictionary
    assert len(SUBSECTOR_DEFINITION) == 11                      # 11 named + 'Other' = 12 labels
    sql = _subsector_def_case()
    assert sql.startswith("CASE b.nteev2_subsector ")
    # dashboard-canonical labels that deliberately DIFFER from bmf's raw column
    assert "WHEN 'EDU' THEN 'Education (minus Universities)'" in sql
    assert "WHEN 'HEL' THEN 'Health (minus Hospitals)'" in sql
    # UNU has no WHEN -> folds into the 'Other' catch-all (keeps it null-free)
    assert "'UNU'" not in sql
    assert sql.rstrip().endswith("ELSE 'Other' END")


# ---- source='bmf' (org-level registry) mode ----------------------------------
def test_bmf_mode_build_sql_drops_the_core_join():
    sql, params = _build_sql(["ein", "geo_state_abbr"], {}, BMF_SRC, [])
    assert 'ON c."ein" = b."ein"' not in sql      # no CORE driving join
    assert "FROM (" in sql                          # driven by the _bmf_source() subquery
    assert params == []


def test_core_mode_build_sql_keeps_the_core_join():
    sql, _ = _build_sql(["ein"], {}, SRC, ["s3://x/core_2019_990.parquet"])
    assert 'ON c."ein" = b."ein"' in sql           # CORE still drives in the default mode


def test_bmf_active_years_compiles_to_lifespan_overlap():
    sql, params = _build_sql(["ein"], {}, BMF_SRC, [], active_years=[2018, 2019, 2022])
    assert '"first_year_in_bmf" <= ?' in sql and '"last_year_in_bmf" >= ?' in sql
    assert params == [2022, 2018]                   # overlap with [min,max] of the span, NOT an IN
    assert 'ON c."ein" = b."ein"' not in sql


def test_bmf_active_years_ands_with_in_filters():
    sql, params = _build_sql(["ein"], {"geo_state_abbr": ["CA"]}, BMF_SRC, [], active_years=[2020])
    assert " AND " in sql
    assert params == ["CA", 2020, 2020]


def test_with_provenance_cols_forces_both_lifespan_cols_when_filtering():
    # active_years applied: both lifespan cols appended (deduped), result self-audits the overlap
    assert _with_provenance_cols(["ein"], [2020]) == ["ein", "first_year_in_bmf", "last_year_in_bmf"]
    assert _with_provenance_cols(["ein", "last_year_in_bmf"], [2020]) == \
        ["ein", "last_year_in_bmf", "first_year_in_bmf"]


def test_with_provenance_cols_noop_without_active_years():
    # no overlap filter (core mode, or BMF with no year filter): columns untouched
    assert _with_provenance_cols(["ein", "geo_state_abbr"], None) == ["ein", "geo_state_abbr"]
    assert _with_provenance_cols(["ein"], []) == ["ein"]


def test_validate_active_years_rules():
    assert _validate_active_years({}) is None
    assert _validate_active_years({"active_years": [2019, 2020]}) == [2019, 2020]
    with pytest.raises(BadRequest):                 # tax_years is a CORE concept; reject in BMF mode
        _validate_active_years({"tax_years": [2019], "active_years": [2019]})
    with pytest.raises(BadRequest):
        _validate_active_years({"active_years": "2019"})
    with pytest.raises(BadRequest):
        _validate_active_years({"active_years": [2019.5]})


def test_validate_bmf_mode_skips_tax_years_but_keeps_ein():
    years, forms, cols, filters, fmt = _validate({"columns": ["geo_state_abbr"]}, BMF_SRC, "bmf")
    assert years == [] and forms == []
    assert cols[0] == "ein"                         # the key is still force-included
