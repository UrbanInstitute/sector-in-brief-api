import pytest
from query.query import format_db_query, _generate_filters, _generate_select_vars, format_dd_query

def test_format_db_query():
    table = "test_table"
    parsed_params = {
        "filterParameters": {"TAX_YEAR": ["2020", "2021"], "STATE": ["CA", "NY"]},
        "varParameters": {"var": ["column1", "column2"]}
    }
    expected = """SELECT column1, column2
    FROM test_table
    WHERE TAX_YEAR IN (2020, 2021) AND\nSTATE IN ('CA', 'NY')"""
    
    result = format_db_query(table, parsed_params)
    assert result.strip() == expected.strip()

def test_generate_filters():
    # Test numeric filters
    params = {"filterParameters": {"TAX_YEAR": ["2020", "2021"], "ASSET_SIZE": ["100", "200"]}}
    expected = "TAX_YEAR IN (2020, 2021) AND\nASSET_SIZE IN (100, 200)"
    assert _generate_filters(params) == expected
    
    # Test string filters
    params = {"filterParameters": {"STATE": ["CA", "NY"], "TYPE": ["A", "B"]}}
    expected = "STATE IN ('CA', 'NY') AND\nTYPE IN ('A', 'B')"
    assert _generate_filters(params) == expected
    
    # Test empty filters
    params = {"filterParameters": {}}
    assert _generate_filters(params) == ""

def test_generate_select_vars():
    params = {"varParameters": {"var": ["col1", "col2", "col3"]}}
    
    # Test Athena format
    assert _generate_select_vars(params, "Athena") == "col1, col2, col3"
    
    # Test S3 format
    assert _generate_select_vars(params, "S3") == "col1', 'col2', 'col3"
    
    # Test invalid resource
    with pytest.raises(ValueError):
        _generate_select_vars(params, "Invalid")

def test_edge_cases():
    # Test single value filters
    params = {"filterParameters": {"TAX_YEAR": ["2020"]}, "varParameters": {"var": ["col1"]}}
    result = _generate_filters(params)
    assert result == "TAX_YEAR IN (2020)"
    
    # Test special characters in values
    params = {"filterParameters": {"STATE": ["O'Connor", "Smith-Jones"]}, 
             "varParameters": {"var": ["col1"]}}
    result = _generate_filters(params)
    assert result == "STATE IN ('O'Connor', 'Smith-Jones')"

def test_format_dd_query():
    parsed_params = {
        "varParameters": {"var": ["column1", "column2", "column3"]}
    }
    var_column = "VARIABLE_NAME"
    
    expected = """SELECT * 
    FROM s3object s 
    WHERE VARIABLE_NAME IN 
    ('column1', 'column2', 'column3')
    """
    
    result = format_dd_query(parsed_params, var_column)
    assert result.strip() == expected.strip()

def test_format_dd_query_single_var():
    parsed_params = {
        "varParameters": {"var": ["single_column"]}
    }
    var_column = "VARIABLE_NAME"
    
    expected = """SELECT * 
    FROM s3object s 
    WHERE VARIABLE_NAME IN 
    ('single_column')
    """
    
    result = format_dd_query(parsed_params, var_column)
    assert result.strip() == expected.strip()

def test_format_dd_query_special_chars():
    parsed_params = {
        "varParameters": {"var": ["col.1", "col#2", "col$3"]}
    }
    var_column = "VARIABLE_NAME"
    
    expected = """SELECT * 
    FROM s3object s 
    WHERE VARIABLE_NAME IN 
    ('col.1', 'col#2', 'col$3')
    """
    
    result = format_dd_query(parsed_params, var_column)
    assert result.strip() == expected.strip()