"""Unit tests for the new /data handler's pure logic (no AWS / DuckDB calls)."""
import os
os.environ.setdefault("RESULTS_BUCKET", "test-bucket")  # query.py reads this at import

import pytest
from query.query import (_validate, _build_sql, _dictionary_sql, _core_parquets,
                         _sql_list, BadRequest)

# fake column->source map (core wins overlaps): ein/total_revenue core; the rest bmf
SRC = {"ein": "c", "total_revenue": "c",
       "geo_state_abbr": "b", "org_name_display": "b", "ntee_common_code": "b"}


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
