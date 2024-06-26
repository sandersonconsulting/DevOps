import os
import boto3
import botocore.session
import requests
import json

# Config file path
config_file_path = 'config.json'

def load_config():
    try:
        with open(config_file_path, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        print(f"Config file '{config_file_path}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON in config file '{config_file_path}'.")
        return None

def get_bucket_size(bucket_name, session):
    total_size = 0
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    for page in response_iterator:
        for obj in page.get('Contents', []):
            total_size += obj['Size']

    return total_size

def list_bucket_sizes(session, splunk_config):
    s3 = session.client('s3')
    response = s3.list_buckets()

    for bucket in response['Buckets']:
        bucket_name = bucket['Name']
        bucket_size = get_bucket_size(bucket_name, session)
        send_to_splunk(bucket_name, bucket_size, splunk_config)

def send_to_splunk(bucket_name, bucket_size, splunk_config):
    if not splunk_config:
        print("Splunk configuration not found.")
        return

    splunk_hec_url = splunk_config.get('hec_url')
    splunk_token = splunk_config.get('token')
    
    if not splunk_hec_url or not splunk_token:
        print("Splunk HEC URL or token not found in config.")
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
    splunk_config = load_config().get('splunk')
    list_bucket_sizes(session, splunk_config)
