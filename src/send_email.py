def send_email(ses_client, recipient, file_url, sender_email):
    """
    Send an email to the recipient with a link to download the query results.
    Args:
        ses_client: A Boto3 SES client
        recipient: The email address of the recipient
        file_url: The URL to download the query results
        sender_email: The email address of the sender
    Returns:
        A string indicating the status of the email sent
    """
    sender = f"{sender_email}"
    subject = "Your Data Request is Ready"
    body_text = f"Your query results are ready. You can download them using this link: {file_url}"
    body_html = f"""<html>
    <head></head>
    <body>
        <h1>Your Data Request Results Are Ready</h1>
        <p>Your query results are ready. You can download them using this link:</p>
        <p><a href='{file_url}'>Download Query Results</a></p>
        <p>This link will expire in 1 hour.</p>
    </body>
    </html>
    """
    
    try:
        response = ses_client.send_email(
            Destination={'ToAddresses': [recipient]},
            Message={
                'Body': {
                    'Html': {'Data': body_html},
                    'Text': {'Data': body_text},
                },
                'Subject': {'Data': subject},
            },
            Source=sender
        )
    except ClientError as e:
        return(f"An error occurred while sending email: {e.response['Error']['Message']}")
    else:
        return(f"Email sent! Message ID: {response['MessageId']}")