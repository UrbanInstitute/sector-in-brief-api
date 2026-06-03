# Setup code for AWS Athena Pointer
import boto3
import os
from botocore.exceptions import ClientError
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

def athena_setup(bucket, file_key, results_key, table_name, csv_file):
    """
    Create an Athena table from a Parquet file in S3
    Args:
        bucket: The name of the S3 bucket
        file_key: The key of the Parquet file in the S3 bucket
        results_key: The key of the results in the S3 bucket
        table_name: The name of the table to create
    Returns:
        The query execution ID
    """
    athena = boto3.client('athena')
    s3 = boto3.client('s3')
    s3_path = f"s3://{bucket}/{file_key}"

    query = create_table_query(csv_file, table_name, s3_path)
    print(query)
    try:
        # Execute the query
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': f's3://{bucket}/{results_key}'
            }
        )
        
        print(f"Table {table_name} created successfully")
        return response['QueryExecutionId']
    except ClientError as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    athena_setup(bucket = "nccs-dataexplorer-prod", 
                 file_key="data/download/V0.1/panel/",
                 results_key="query/",
                 table_name= "CORE_FULL_V0_1",
                 csv_file= "data/core_schema.csv")