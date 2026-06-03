def where_statement(filter_params):
    """
    Generate a WHERE clause for a SQL query based on the filter parameters provided.
    Args:
        filter_params: A dictionary of column-value pairs to filter the query by
    Returns:
        The WHERE clause as a string
    """
    if not filter_params:
        raise ValueError("Query parameters cannot be empty")
    
    conditions = []
    for column, value in filter_params.items():
        if isinstance(value, list):
            conditions.append(f"{column} IN ({','.join(['%s' for _ in value])})")
        else:
            conditions.append(f"{column} = %s")
    
    where_clause = " AND ".join(conditions)
    
    where_statement = f"WHERE {where_clause}"
    
    # Replace placeholders with actual values
    for column, value in filter_params.items():
        if isinstance(value, list):
            for v in value:
                query = query.replace("%s", f"'{v}'", 1)
        else:
            query = query.replace("%s", f"'{value}'", 1)
    
    return where_statement