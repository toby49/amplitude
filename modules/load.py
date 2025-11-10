import boto3
import re
import os
from datetime import datetime, timedelta, date
from pathlib import Path

def list_s3_files(bucket_name, aws_access_key_id, aws_secret_access_key, s3_folder=""):
    """
    List all files in an S3 bucket folder.
    
    Args:
        bucket_name (str): S3 bucket name
        aws_access_key_id (str): AWS access key
        aws_secret_access_key (str): AWS secret key
        s3_folder (str): Folder path (e.g., "my-folder/")
    
    Returns:
        list: File names in the folder
    """
    s3 = boto3.client('s3', 
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_folder):
        if 'Contents' in page:
            files.extend([obj['Key'] for obj in page['Contents']])
    
    return files



def find_missing_amplitude_data(bucket_name, aws_access_key_id, aws_secret_access_key, s3_folder, start_date=None, end_date=None):
    """
    Check S3 bucket for missing Amplitude data files.

    Args:
        bucket_name (str): S3 bucket name
        aws_access_key_id (str): AWS access key
        aws_secret_access_key (str): AWS secret key
        s3_folder (str): S3 folder prefix
        start_date (date, optional): Start date. Defaults to 1 week before yesterday
        end_date (date, optional): End date. Defaults to yesterday

    Returns:
        list: List of tuples (start_date, end_date) for missing data ranges
    """
    # Ensure end_date is a date object
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    elif isinstance(end_date, datetime): # If it's a datetime object, convert to date
        end_date = end_date.date()

    # Ensure start_date is a date object (if provided)
    if start_date is not None and isinstance(start_date, datetime):
        start_date = start_date.date()

    # Fix folder path
    if not s3_folder.endswith('/'):
        s3_folder += '/'

    # Get files from S3
    files = list_s3_files(bucket_name, aws_access_key_id, aws_secret_access_key, s3_folder)

    # Set start_date default based on files
    if start_date is None:
        file_dates = []
        for file in files:
            match = re.search(r'(\d{4}-\d{2}-\d{2})_(\d+)', file)
            if match:
                date_str = match.groups()[0]
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                file_dates.append(file_date)

        if file_dates:
            start_date = min(file_dates)
        else:
            start_date = end_date - timedelta(days=7)

    # Extract existing dates and hours
    existing_hours = set()
    for file in files:
        match = re.search(r'(\d{4}-\d{2}-\d{2})_(\d+)', file)
        if match:
            date_str, hour_str = match.groups()
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date() # file_date is a datetime.date object
            hour = int(hour_str)

            # Only keep files in our date range
            # At this point, start_date, file_date, and end_date should all be datetime.date objects
            if start_date <= file_date <= end_date:
                dt = datetime.combine(file_date, datetime.min.time()) + timedelta(hours=hour)
                existing_hours.add(dt)

    # Generate all required hours
    # These conversions are correct for creating datetime objects for hour-level granularity
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(hours=23)

    required_hours = []
    current = start_dt
    while current <= end_dt:
        required_hours.append(current)
        current += timedelta(hours=1)

    # Find missing hours
    missing_hours = [h for h in required_hours if h not in existing_hours]

    if not missing_hours:
        return []

    # Group into ranges (max 7 days each)
    ranges = []
    start = missing_hours[0]
    end = missing_hours[0]

    for hour in missing_hours[1:]:
        if hour == end + timedelta(hours=1) and (hour - start).days < 7:
            end = hour  # Extend range if consecutive and under 7 days
        else:
            ranges.append((start.strftime('%Y%m%dT%H'), end.strftime('%Y%m%dT%H')))
            start = end = hour

    # Add final range
    ranges.append((start.strftime('%Y%m%dT%H'), end.strftime('%Y%m%dT%H')))

    return ranges

def upload_to_s3(bucket_name, aws_access_key,aws_secret_key,local_folder,s3_folder):

    # Assign slashes if missing
    local_folder = local_folder if local_folder.endswith('/') else local_folder + '/'
    s3_folder = s3_folder if s3_folder.endswith('/') else s3_folder + '/'

    # Create S3 Client using AWS Credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    files_to_upload = []
    for root, _, filenames in os.walk(local_folder):
        files_to_upload.extend(filenames)

    for file in files_to_upload:
        aws_file_destination = s3_folder + file
        local_path = local_folder + file
        s3_client.upload_file(local_path, bucket_name, aws_file_destination)
        print(f"✓ Uploaded: {file}")

def cleanup_uploaded_files(local_folder, s3_files):
    """
    Delete local files that exist in S3, rename folder if files remain.
    
    Args:
        local_folder (str): Local folder path
        s3_files (list): List of S3 filenames (without path)
    """
    local_path = Path(local_folder)
    if not local_path.exists():
        return
    
    local_files = [f.name for f in local_path.iterdir() if f.is_file()]
    s3_set = set(s3_files)
    
    # Delete files that are in S3
    remaining_files = []
    for filename in local_files:
        if filename in s3_set:
            (local_path / filename).unlink()
        else:
            remaining_files.append(filename)
    
    # Rename folder if files remain, otherwise remove empty folder
    if remaining_files:
        local_path.rename(local_path.parent / "data_failed_to_upload")
    else:
        local_path.rmdir()