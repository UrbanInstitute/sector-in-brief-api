from send_email import send_email

def lambda_handler(event, context):
    """
    Lambda function that processes a user's data request and sends an email with the results.

    Args:
        event: A dictionary of input parameters including user data, variables, and filters
        context: The runtime information of the Lambda function
    Returns:
        A json file containing the status code and a message indicating the status of the email sent
    """
    # Parse input
    table_name = "nccs_core"
    bucket_name = "nccsdata"
    result_key = "dataexplorer/api/results/"

    body = json.loads(event['body'])
    user_data = body["user"]
    variables = body["variables"]
    filters = body["filters"]

    # Initialize AWS clients
    athena = boto3.client('athena')
    s3 = boto3.client('s3')
    ses = boto3.client('ses')

    # Construct Athena query
    where_statement = where_statement(filters)
    query = f"""
    SELECT {', '.join(variables)}
    FROM {table_name}
    {where_statement}
    """

    # Start Athena query execution
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://{bucket_name}/{result_key}'
        }
    )

    # Get query execution ID
    query_execution_id = response['QueryExecutionId']

    # Wait for query to complete
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if state == 'SUCCEEDED':
        # Get the S3 path of the results
        s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']

        # Generate presigned URL
        url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': 'nccsdata',
                    'Key': f"{result_key}{query_execution_id}.csv"
                },
                ExpiresIn=3600  # URL expires in 1 hour
            )

        # Send email
        email_status = send_email(ses, user_data['email'], url, sender_email)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data processed and email sent successfully!')
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps('Query execution failed')
        }