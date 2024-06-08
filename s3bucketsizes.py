import boto3
import requests
import json

# AWS credentials and region
aws_access_key = 'YOUR_AWS_ACCESS_KEY'
aws_secret_key = 'YOUR_AWS_SECRET_KEY'
aws_region = 'YOUR_AWS_REGION'

# Splunk HEC configuration
splunk_hec_url = 'YOUR_SPLUNK_HEC_URL'
splunk_token = 'YOUR_SPLUNK_HEC_TOKEN'

def get_bucket_size(bucket_name):
    total_size = 0
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    for page in response_iterator:
        for obj in page.get('Contents', []):
            total_size += obj['Size']

    return total_size

def list_bucket_sizes():
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
    response = s3.list_buckets()

    for bucket in response['Buckets']:
        bucket_name = bucket['Name']
        bucket_size = get_bucket_size(bucket_name)
        send_to_splunk(bucket_name, bucket_size)

def send_to_splunk(bucket_name, bucket_size):
    headers = {'Authorization': f'Splunk {splunk_token}'}
    data = {
        "event": {
            "bucket_name": bucket_name,
            "bucket_size": bucket_size
        }
    }
    response = requests.post(splunk_hec_url, headers=headers, json=data, verify=False)
    if response.status_code != 200:
        print(f"Failed to send data to Splunk: {response.text}")

if __name__ == "__main__":
    list_bucket_sizes()
