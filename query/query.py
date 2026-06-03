# Setup

import boto3
import logging
import json
import os
import logging
import time
import hashlib
import itertools
from datetime import datetime
import uuid
import pandas as pd
import requests
from botocore.exceptions import ClientError

# set logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create AWS service clients
athena = boto3.client('athena')
s3 = boto3.client('s3')
ses = boto3.client('ses')

OUTPUT_BUCKET_PATH = "s3://{bucket}/extracts/{hash}/"

def _get_workflow(params):
    """
    Direct stg/prod workflow via API Gateway event.

    Args:
        params (dict): Parsed query parameters from AWS API Gateway

    Returns: A tuple of names for the Athena table, S3 Bucket, and location for
    results.
    """
    if params["stage"] == "stg":
        table = "CORE_FULL_V0_1"
        bucket = "nccs-dataexplorer-stg"
        result_key = "results"
    else:
        table = "CORE_FULL_V0_1"
        bucket = "nccs-dataexplorer-prod"
        result_key = "results"

    return table, bucket, result_key

# helper functions for error handling and formatting
def handle_api_error(func):
    """
    Wrapper to (attempt to) gracefully capture and return API errors.
    
    Args:
        func: The function to be called
    
    Returns:
        Function call if successful, 400 code with error if exception is raised.
    """
    def wrapper_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if str(e).startswith("API ERROR"):
                body = str(e).replace("API ERROR: ", "")
            else:
                body = "There was an error with your request. Please consider filing an issue at https://github.com/UrbanInstitute/education-data-summary-endpoints/issues if the problem persists."
            logger.error({"event" : args[0], "exception" : e})
            return {
                    "statusCode": 400,
                    "body": json.dumps([{"API ERROR": body}])
                    }
    return wrapper_func

def send_email(db_url, dd_url, recipient):
    """
    Formats the output message of the Lambda function and send email with AWS SES
    
    Args:
        db_url (str): The URL to the query results in S3.
        dd_url (str): The URL to the data dictionary query results in S3.
        recipient (str): The email address of the recipient.
    
    Returns:
        Dictionary of formatted response data.
    """

    subject = "Your Data Request is Ready"
    body_text = f"Your query results are ready. \
        You can download the 990 data set using this link: {dd_url}. \
        You can download the data dictionary using this link: {db_url}. \
        This link will expire in 1 week. Please check your spam folder if you \
        do not see the email in your inbox. Contact us at nccs@urban.org if you \
        have any questions."
    body_html = f"""<html>
    <head></head>
    <body>
        <h1>Your Data Request Results Are Ready</h1>
        <p>Your query results are ready. You can download the dataset using this link:</p>
        <p><a href='{db_url}'>Download Query Results</a></p>
        <p> You can also download the data dictionary using this link:</p>
        <p><a href='{dd_url}'>Download Data Dictionary</a></p>
        <p>This link will expire in 1 week.</p>
        <p>Please check your spam folder if you do not see the email in your inbox.</p>
        <p>Contact us at nccs@urban.org if you have any questions.</p>
    </body>
    </html>
    """
    try:
        response = ses.send_email(
            Destination={'ToAddresses': [recipient]},
            Message={
                'Body': {
                    'Html': {
                        'Data': body_html,
                        "Charset": "UTF-8"
                    },
                    'Text': {
                        'Data': body_text,
                        "Charset": "UTF-8"
                    },
                },
                'Subject': {
                    'Data': subject,
                    "Charset": "UTF-8"
                },
            },
            Source="Thiyaghessan <tpoongundranar@urban.org>"
        )
    except ClientError as e:
        return(f"An error occurred while sending email: {e.response['Error']['Message']}")
    else:
        return(f"Email sent! Message ID: {response['MessageId']}")            
                    
# helper functions to parse and validate api query string parameters
def _parse_parameters(unparsed_params, sep = "-"):
    """
    Parses original query string parameters for validation and formatting.
    
    Args:
        unparsed_params (dict): The query string parameters from an API Gateway event.
        sep (string): The separator used when defining a numeric range.
    
    Returns:
        A dictionary of parsed and formatted query string parameters.
    """
    parsed_parameters = {
        key : list(
            itertools.chain.from_iterable(
                [
                    [str(x) for x in list(range(int(param.split(sep)[0]),
                                           int(param.split(sep)[1])+1))
                    ]
            if sep in param
            else [param]
            for param in value.split(",")
            ]))
        for key, value in unparsed_params.items()
        if key != "mode"
    }

    return parsed_parameters

def _check_parameters(params):
    """
    Check that required parameters are present in the query string.
    
    Args:
        params (dict): The query string parameters from API Gateway event.
    
    Returns:
        None
    
    Raises:
        Exception: If required parameters are not present in the query string.
    """
    required_parameters = ["vars"]

    if not all(param in params.keys() for param in required_parameters):
        raise Exception("API ERROR: Query string parameters 'vars' must be specified")

# helper functions for pulling api metadata
def _get_api_endpoints(url_stem):
    """
    Get a list of all api endpoints available.
    
    Args:
        url_stem (str): The educationdata url stem to use (i.e., production or staging).
    
    Returns:
        A list of endpoint ids.
    """
    r = requests.get(f"{url_stem}/api/v1/api-endpoints/")
    results = r.json()["results"]
    api_endpoints = [result["endpoint_id"] for result in results]
    return api_endpoints

def _validate_filter_args(parsed_params, varlist):
    """
    Ensures that additional filter arguments are actually present to be
    queried.
    
    Args:
        parsed_params (dict): Parsed and formatted query string parameters.
        varlist (dict): The varlist from the topic endpoint of interest.
    
    Returns:
        None
    
    Raises:
        Exception: If argument does not exist to be queried.
    """
    req_args = ["vars"]
    filter_args = [key for key in parsed_params.keys() if key not in req_args]

    for arg in filter_args:
        if arg not in varlist.keys():
            raise Exception(f"API ERROR: Query string parameter {arg} is not a valid")

def _validate_vars_arg(parsed_params, varlist):
    """
    Ensures that value passed to 'var' argument is valid to be queried.
    
    Args:
        parsed_params (dict): Parsed and formatted query string parameters.
        varlist (dict): The varlist from the topic endpoint of interest.
    
    Returns:
        None
    
    Raises:
        Exception: If argument is not valid to be queried.
    """
    vars = parsed_params['vars']
    error = f"API ERROR: '{vars}' is not a valid 'var' argument"

    if vars not in varlist.keys():
        raise Exception(error)
    else:
        return None

def _validate_filter_values(parsed_params, varlist):
    """
    Ensures that values passed to additional filter arguments are actually
    valid to be queried.
    
    Args:
        parsed_params (dict): Parsed and formatted query string parameters.
        varlist (dict): The varlist from the topic endpoint of interest.
    
    Returns:
        None
    
    Raises:
        Exception: If argument is not valid to be queried.
    """
    req_args = ["vars"]

    filter_vars = {key: value for key, value in parsed_params.items() if key not in req_args}

    for key, value in filter_vars.items():
        unparsed_valid_values = varlist[key]['values']
        parsed_valid_values = [value.split(":")[0].strip() for value in unparsed_valid_values.replace('"', '').split(",")]
        if not all(x in parsed_valid_values for x in value):
            raise Exception(f"API ERROR: Invalid value for {key}")

def _generate_select_vars(params, resource):
    """
    Matches select vars to correct table and generates string to be used when 
    formatting query.
    
    Args:
        params (dict): Parsed and formatted query string parameters.
        resource (str): The resource to be queried (i.e., Athena or S3).
    Returns:
        Formatted string to be used in an Athena query.
    """
    select_vars = params["varParameters"]["var"]
    if resource == "Athena":
        return ", ".join(select_vars)
    elif resource == "S3":
        return "', '".join(select_vars)
    

def _generate_filters(params):
    """
    Matches filter vars to correct Athena table and generates string to be used
    when formatting Athena query.
    
    Args:
        params (dict): Parsed and formatted query string parameters.
    Returns:
        Formatted string to be used in an Athena query.
    """
    
    filter_params = {key: value
                    for key, value in params["filterParameters"].items()
                    }

    filters = []

    filters += [
        "{var} IN ({value})".format(var = key, value = ", ".join(value))
        if key in ["TAX_YEAR", "Size"]
        else "{var} IN ('{value}')".format(var = key, value = "', '".join(value))
        for key, value in filter_params.items()
        ]

    filter_string = " AND\n".join(filters)

    return filter_string

def format_db_query(table, parsed_params):    
    """
    Matches filter vars to correct Athena table and generates string to be used
    when formatting Athena query.
    
    Args:
        table (string): Athena table to be queried.
        parsed_params (dict): Parsed and formatted query string parameters.
    
    Returns:
        Formatted string to be used in an Athena query.
    """
    QUERY_SHELL = """SELECT DISTINCT {vars}
    FROM {table}
    WHERE {filters}
    """
    filter_vars = _generate_filters(parsed_params)
    select_vars = _generate_select_vars(parsed_params, resource = "Athena")

    # format query
    query = QUERY_SHELL.format(table = table,
                               filters = filter_vars,
                               vars = select_vars)

    return query

def format_dd_query(parsed_params, var_column):
    """
    Create query for S3 Select that retrives data dictionary.

    Args:
        parsed_params (dict): Parsed and formatted query string parameters.
        var_column (str): The column to be queried in the data dictionary.
    """
    # TODO: Add default columns for select_vars
    select_vars = _generate_select_vars(parsed_params, resource="S3")
    QUERY_SHELL = """SELECT * 
    FROM s3object s 
    WHERE s._1 IN ('{vars}')
    OR s._1 = '{var_column}'
    """
    query = QUERY_SHELL.format(var_column = var_column,
                               vars = select_vars)

    return query

def s3_select(query:str, bucket:str, dd_key:str, results_key:str, filename:str):
    """
    Execute S3 Select query on the data dictionary and save the results to an 
    S3 bucket.

    Args:
        query (str): The SQL query to be executed.
        bucket (str): The S3 bucket name.
        dd_key (str): The S3 key of the data dictionary.
        results_key (str): The S3 key to save the results to.
        filename (str): The filename to save the results as.

    Returns:
        str: The URL to the query results in S3.
    """
    s3_response = s3.select_object_content(
        Bucket=bucket,
        Key=dd_key,
        ExpressionType='SQL',
        Expression=query,
        InputSerialization={
            'CSV': {
                'FileHeaderInfo': 'NONE',
                'FieldDelimiter': ',',
                'QuoteCharacter': '"',
                'QuoteEscapeCharacter': '"',
                'RecordDelimiter': '\n',
                'Comments': '#',
                'AllowQuotedRecordDelimiter': True
            }
        },
        OutputSerialization={
            'CSV': {
                'FieldDelimiter': ',',
                'QuoteFields': 'ASNEEDED',
                'QuoteCharacter': '"',
                'QuoteEscapeCharacter': '"'
            }
        }
    )
    logger.info(f"S3 Select Response: {s3_response}")
    records = []
    for event in s3_response['Payload']:
        if 'Records' in event:
            records.append(event['Records']['Payload'].decode('utf-8'))
    output_filename = generate_unique_filename(prefix=filename, extension='csv')
    output_key = f"{results_key}/{output_filename}"
    s3.put_object(Bucket=bucket, Key=output_key, Body=''.join(records))
    logger.info(f"Data dictionary query results saved to {bucket}/{output_key}")
    url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket,
                    'Key': output_key
                },
                ExpiresIn=86400  # URL expires in 24 hours
            )
    return url

def generate_unique_filename(prefix: str = None, extension: str = 'csv') -> str:
    """
    Generate a unique filename using timestamp and UUID.
    
    Args:
        prefix (str, optional): Prefix for the filename
        extension (str, optional): File extension without dot
    
    Returns:
        str: Unique filename
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = str(uuid.uuid4())[:8]  # Use first 8 characters of UUID
    
    if prefix:
        return f"{prefix}/{timestamp}_{unique_id}.{extension}"
    return f"{timestamp}_{unique_id}.{extension}"

def _check_s3(query, url_stem, bucket, query_hash, dtype):
    """
    Reads results directly from S3 if the AWS Athena query has already been run.

    Args:
        query (str): The AWS Athena query to check.
        url_stem (str): The educationdata url stem to use (i.e., production or staging).
        bucket (str): An S3 bucket name (i.e., ed-data-csv or ed-data-csv-dev).
        query_hash (str): A sha256 hashed query.
        dtype (dict): Dictionary to be used as the `dtype` argument in `pd.read_csv`.

    Returns:
        Dictionary of query results, formatted as json records.
    """
    response = s3.list_objects_v2(Bucket = bucket,
                                  Prefix = "extracts/{hash}".format(hash = query_hash))

    if response["KeyCount"] > 0:
        key = [obj['Key']
               for obj in response['Contents']
               if os.path.splitext(obj['Key'])[1] == ".csv" ][0]

        results_path = f"s3://{bucket}/{key}"
        bucket, key = results_path.replace("s3://", "").split("/", 1)
        url = f"{url_stem}/csv/{key}"
        data = pd.read_csv(results_path, dtype=dtype)
        results = {"data":data, "url":url}
    else:
        results = None

    return results


def retrieve_query(client, url_stem, query_response):
    """
    Retrieves a query result from AWS Athena.

    Args:
        client (boto3): boto3 low-level service client for AWS Athena.
        url_stem (str): The educationdata url stem to use (i.e., production or staging).
        query_response (dict): Response object from a call to
            client.start_query_execution.

    Returns:
        A dictionary of query results, formatted as json records.

    Raises:
        Exception: If query fails or is cancelled.
    """
    query_execution = client.get_query_execution(QueryExecutionId=query_response["QueryExecutionId"])
    query_status = query_execution['QueryExecution']['Status']['State']

    if query_status == 'FAILED' or query_status == 'CANCELLED':
        raise Exception('API ERROR: Failed Athena query')
    elif query_status == 'SUCCEEDED':
        results_path = query_execution['QueryExecution']['ResultConfiguration']['OutputLocation']
        bucket, key = results_path.replace("s3://", "").split("/", 1)
        url = f"{url_stem}/csv/{key}"
        query = query_execution['QueryExecution']['Query']
        results = {"url":url}
        return(results)
    else:
        time.sleep(0.20)
        return(retrieve_query(client, url_stem, query_response, dtype))

def save_query_params(params, bucket, key_root, query_execution_id):
    """
    Save query parameters to a log file.

    Args:
        params (dict): Parsed and formatted query string parameters.
        bucket (str): An S3 bucket name (i.e., ed-data-csv or ed-data-csv-dev).
        key_root (str): The root key for the log file.
        query_execution_id (str): The query execution ID from AWS Athena.
    """
    json_str = json.dumps(params, indent=2)
    key = f"{key_root}{query_execution_id}.json"

    s3.put_object(Bucket=bucket, 
                  Key=key, 
                  Body=json_str)
    
    logger.info(f"Query parameters saved to {bucket}/{key}")
    return None

### lambda handler ------------------------------------------------------------

# main function lambda calls
@handle_api_error
def lambda_handler(event, context):
    """
    Provide an event that contains the following keys
    """
    # Start timer for performance metrics
    t0 = time.time()
    logger.info(f"Event: {event}")

    # Pull parameters needed for S3 Select and Athena Queries
    event_params = json.loads(event['body'])
    # TODO: Check and debug parameters from AWS Gateway event
    # _check_parameters(unparsed_params)
    # TODO: Decide if parsing step is necessary (Ask Erika)
    # params = _parse_parameters(unparsed_params)
    recipient = event_params["requestContext"]["userEmail"]
    params = event_params["queryParameters"]
    # Add function to save params to a log file
    logger.info(f"Params: {params}")

    table, bucket, result_key = _get_workflow(params)
    logger.info(f"Table: {table}, Bucket: {bucket}")

    # Generate queries 
    # TODO: Decide if a unique hash is necessary (ask Erika)
    # query_hash = hashlib.sha256(query.encode('utf-8')).hexdigest()
    # Create Athena query
    athena_query = format_db_query(table, params)
    logger.info(f"Athena Query: {athena_query}")
    s3_query = format_dd_query(params, var_column = "variable_name")
    logger.info(f"S3 Select Query: {s3_query}")

    # Run AWS Athena query on table
    t1 = time.time()
    query_response = athena.start_query_execution(
        QueryString = athena_query,
        ResultConfiguration = {
            "OutputLocation" : f"s3://{bucket}/{result_key}/"
        }
    )
    # retrieve results
    query_execution_id = query_response['QueryExecutionId']
    # Save query to the queries/ folder of an S3 Bucket for metrics
    save_query_params(params = event_params, 
                      bucket = bucket, 
                      key_root = "queries/", 
                      query_execution_id = query_execution_id)
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
    if state == 'SUCCEEDED':
        # Get the S3 path of the results
        # TODO: Find out how to refactor the s3 path for the results
        s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        logger.info(f"Query results saved to {s3_path}")
    # Generate URL for Athena query results
    try:
        db_url = f"https://{bucket}.s3.us-east-1.amazonaws.com/{result_key}/{query_execution_id}.csv"
    except ClientError as e:
        db_url = f"https://{bucket}.s3.amazonaws.com/{result_key}/{query_execution_id}.csv"
    except Exception as e:
        logger.error(f"Error generating URL: {e}")
        # Generate a presigned URL for the query results
        try:
            db_url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': f"{bucket}",
                    'Key': f"{result_key}/{query_execution_id}.csv"
                },
                ExpiresIn=604800  # URL expires in 1 Week
            )
        except Exception as e:
            logger.error(f"Error generating presigned URL: {e}")
            # Fallback to generating a presigned URL
    t2 = time.time()
    athena_time = t2 - t1

    # Run S3 Select query on data dictionary
    t3 = time.time()
    dd_url = s3_select(query = s3_query, 
                       bucket = bucket, 
                       dd_key = f"data/download/data_dictionary/panel_dd.csv",
                       results_key = f"{result_key}", 
                       filename = "panel_dd")
    logger.info(f"Data Dictionary Query URL: {dd_url}")
    t4 = time.time()
    s3_select_time = t4 - t3

    # Send email
    email_response = send_email(db_url, dd_url, recipient=recipient)
    logger.info(f"Email Response: {email_response}")
        
    # log
    t5 = time.time()
    total_time = t5 - t0
    logger.info(
        {
            "event" : event, 
            "time" : {
                "total": round(total_time, 2),
                "athena": round(athena_time, 2),
                "s3_select": round(s3_select_time, 2)
            }
        }
    )
    logger.info("Query completed successfully!")

    rs = {
            "statusCode": 200,
            "body": json.dumps({
                "urls": {"db": db_url, "dd": dd_url},
                "email": email_response
            })
        }

    return rs

# TODO: Add test cases for the lambda function
# TODO: Default data dictionary is the full dictionary

    