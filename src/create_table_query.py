import csv 

def create_table_query(csv_file, table_name, s3_location):
    """
    Generate an Athena Query to create the pointer from a schema in a CSV file
    Args:
        csv_file: The path to the CSV file
        table_name: The name of the Athena table
        s3_location: The S3 location where the data is stored
    Returns:
        A SQL Query to create a table with Athena
    """
    athena_types = {
        'VARCHAR': 'string',
        'INTEGER': 'int',
        'DOUBLE': 'double',
        'BIGINT': 'bigint',
        'BLOB': 'binary'
    }

    columns = []

    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row if present
        for row in csv_reader:
            if len(row) >= 3:
                column_name = row[0].strip()
                data_type = row[1].strip().upper()
                athena_type = athena_types.get(data_type, 'string')
                columns.append(f"{column_name} {athena_type}")

    athena_statement = f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (\n"
    athena_statement += ",\n".join(f"    {column}" for column in columns)
    athena_statement += "\n)"
    athena_statement += f"\nSTORED AS PARQUET"
    athena_statement += f"\nLOCATION '{s3_location}'"
    athena_statement += f"\nTBLPROPERTIES ('parquet.compress'='SNAPPY');"

    return athena_statement