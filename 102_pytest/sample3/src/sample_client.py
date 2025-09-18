import requests
import boto3

def fetch_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def save_to_s3(s3_client, bucket, key, data):
    s3_client.put_object(Bucket=bucket, Key=key, Body=str(data))

def fetch_and_store(api_url, s3_client, bucket, key):
    data = fetch_data(api_url)
    save_to_s3(s3_client, bucket, key, data)
    return data