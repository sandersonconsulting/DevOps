import os
import boto3
import botocore.session
import requests
import json

# Splunk HEC configuration
splunk_hec_url_env_variable = 'SPLUNK_HEC_URL'
splunk_token_env_variable = 'SPLUNK_TOKEN'

def get_bucket_size(bucket_name, session):
    total_size = 0
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    for page in response_iterator:
        for obj in page.get('Contents', []):
            total_size += obj['Size']

    return total_size

def list_bucket_sizes(session):
    s3 = session.client('s3')
    response = s3.list_buckets()

    for bucket in response['Buckets']:
        bucket_name = bucket['Name']
        bucket_size = get_bucket_size(bucket_name, session)
        send_to_splunk(bucket_name, bucket_size)

def send_to_splunk(bucket_name, bucket_size):
    splunk_hec_url = os.environ.get(splunk_hec_url_env_variable)
    splunk_token = os.environ.get(splunk_token_env_variable)
    
    if not splunk_hec_url:
        print("Splunk HEC URL environment variable not set.")
        return

    if not splunk_token:
        print("Splunk token environment variable not set.")
        return

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
    session = botocore.session.Session(profile='default')  # Use 'default' profile from ~/.aws/credentials
    list_bucket_sizes(session)
