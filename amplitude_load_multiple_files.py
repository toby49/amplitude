# Retrieve .json files and upload json files to S3 bucket

# Load libraries
import os
import boto3
from dotenv import load_dotenv
from modules.extract_json_files import extract_json_files

# Load .env file
load_dotenv()

# Read .env file
aws_access_key=os.getenv('AWS_ACCESS_KEY')
aws_secret_key=os.getenv('AWS_SECRET_KEY')
aws_bucket_name=os.getenv('AWS_BUCKET_NAME')

# Create S3 Client using AWS Credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Extract the main zip file
zip_path = "data/data.zip"
output_folder = "amp_data"
extract_json_files(zip_path,output_folder) 

files_to_upload = []
for root, _, filenames in os.walk(output_folder):
    files_to_upload.extend(filenames)

for file in files_to_upload:
    aws_file_destination = "python-import/" + file
    output_path = output_folder + "/" + file
    s3_client.upload_file(output_path, aws_bucket_name, aws_file_destination)
    print(f"✓ Uploaded: {file}")