# Test script to trouble shoot Athena queries

import boto3
import os 

os.environ['AWS_PROFILE'] = "672001523455_AWS-DS-AAD"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"

if __name__ == "__main__":
    # Initialize AWS clients
    athena = boto3.client('athena')
    s3 = boto3.client('s3')

    # Construct Athena query
    query = f"""
    SELECT *
    FROM nccs_core
    """

    # Start Athena query execution
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://nccsdata/dataexplorer/api/results/'
        }
    )

    query_execution_id = response['QueryExecutionId']

    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if state == 'SUCCEEDED':
        # Get the S3 path of the results
        s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']

    url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': 'nccsdata',
                    'Key': f"dataexplorer/api/results/{query_execution_id}.csv"
                },
                ExpiresIn=3600  # URL expires in 1 hour
            )

    print(url)