# This script creates the pointer to the table with AWS Athena

# Import libraries
import boto3

athena = boto3.client('athena')
s3 = boto3.client('s3')



