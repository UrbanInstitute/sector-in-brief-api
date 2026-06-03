import boto3
from botocore.exceptions import ClientError
import logging
import math

def copy_large_file(source_bucket, source_key, dest_bucket, dest_key=None, part_size=1024*1024*1024):
    """
    Copy large files between S3 buckets using multipart copy.
    
    Args:
        source_bucket (str): Source bucket name
        source_key (str): Source file path/key
        dest_bucket (str): Destination bucket name
        dest_key (str): Destination file path/key (optional)
        part_size (int): Size of each part in bytes (default 1GB)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if dest_key is None:
        dest_key = source_key
        
    s3_client = boto3.client('s3')
    
    try:
        # Get source file size
        response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
        file_size = response['ContentLength']
        
        # Initiate multipart upload
        mpu = s3_client.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
        upload_id = mpu['UploadId']
        
        # Calculate number of parts needed
        num_parts = math.ceil(file_size / part_size)
        
        # Copy each part
        parts = []
        for part_number in range(1, num_parts + 1):
            # Calculate byte range for this part
            start_byte = (part_number - 1) * part_size
            end_byte = min(start_byte + part_size - 1, file_size - 1)
            
            print(f"Copying part {part_number}/{num_parts}, bytes {start_byte}-{end_byte}")
            
            # Copy this part
            copy_response = s3_client.upload_part_copy(
                Bucket=dest_bucket,
                Key=dest_key,
                PartNumber=part_number,
                UploadId=upload_id,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                CopySourceRange=f'bytes={start_byte}-{end_byte}'
            )
            
            # Add this part to our list of parts
            parts.append({
                'PartNumber': part_number,
                'ETag': copy_response['CopyPartResult']['ETag']
            })
        
        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=dest_bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        print(f"Successfully copied {source_bucket}/{source_key} to {dest_bucket}/{dest_key}")
        return True
        
    except ClientError as e:
        logging.error(f"Error copying object: {e}")
        # Abort the multipart upload if it was started
        if 'upload_id' in locals():
            s3_client.abort_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key,
                UploadId=upload_id
            )
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        # Abort the multipart upload if it was started
        if 'upload_id' in locals():
            s3_client.abort_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key,
                UploadId=upload_id
            )
        return False

# Example usage with progress tracking
def copy_with_size_check(source_bucket, source_key, dest_bucket, dest_key=None):
    """
    Smart copy function that chooses between regular and multipart copy based on file size.
    
    Args:
        source_bucket (str): Source bucket name
        source_key (str): Source file path/key
        dest_bucket (str): Destination bucket name
        dest_key (str): Destination file path/key (optional)
    """
    s3_client = boto3.client('s3')
    
    try:
        # Check file size
        response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
        file_size = response['ContentLength']
        
        # If file is larger than 5GB, use multipart copy
        if file_size > 5 * 1024 * 1024 * 1024:  # 5GB in bytes
            print(f"Large file detected ({file_size/1024/1024/1024:.2f} GB), using multipart copy")
            return copy_large_file(source_bucket, source_key, dest_bucket, dest_key)
        else:
            print(f"Small file detected ({file_size/1024/1024:.2f} MB), using regular copy")
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key or source_key)
            return True
            
    except ClientError as e:
        logging.error(f"Error in copy operation: {e}")
        return False


# Example usage:
if __name__ == "__main__":
    # Single file copy
    copy_with_size_check(
        source_bucket='nccsdata',
        source_key='dataexplorer/api/data/processed/core_full.parquet',
        dest_bucket='nccs-dataexplorer-stg',
        dest_key='data/download/panel/panel.parquet'
    )
