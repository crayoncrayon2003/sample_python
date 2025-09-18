import pytest
from src.sample_client import fetch_and_store
from moto import mock_aws
import boto3

@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        yield client

@pytest.mark.normal
def test_fetch_and_store(requests_mock, s3_client, sample_data):
    api_url = "https://example.com/api/data"
    expected_data = {"foo": "bar", "sample": sample_data["sample"]}
    requests_mock.get(api_url, json=expected_data)

    fetch_and_store(api_url, s3_client, "test-bucket", "data.json")

    obj = s3_client.get_object(Bucket="test-bucket", Key="data.json")
    body = obj["Body"].read().decode()
    assert str(expected_data) == body
    assert "sample" in expected_data
    assert expected_data["sample"] == "data"

@pytest.mark.abnormal
def test_fetch_and_store_api_error(requests_mock, s3_client, sample_data):
    api_url = "https://example.com/api/data"
    requests_mock.get(api_url, status_code=404)

    with pytest.raises(Exception):
        fetch_and_store(api_url, s3_client, "test-bucket", "data.json")