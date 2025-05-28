# services/storage/s3_client.py
"""
S3 client configuration and utility functions for AWS integration.
Handles AWS S3 connection setup, credential management, and S3 path parsing.

This module provides functions to create a configured S3 client using credentials
from environment variables and to parse S3 paths into bucket/key pairs for use
with the AWS SDK. It is used throughout the storage layer for S3 operations.
"""
import os
import boto3
from dotenv import load_dotenv

from config import S3_BUCKET_NAME, AWS_REGION

# Load environment variables
load_dotenv()


def get_s3_client():
    """
    Create and return a configured AWS S3 client.
    
    Reads AWS credentials and region from environment variables and returns a
    boto3 S3 client instance for use in S3 operations.
    
    Returns:
        boto3.client: Configured S3 client
    
    Raises:
        KeyError: If AWS credentials are not found in environment variables
    """
    aws_access_key = os.environ["AWS_ACCESS_KEY"]
    aws_secret_key = os.environ["AWS_SECRET_KEY"]

    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=AWS_REGION,
    )


def parse_s3_path(s3_path):
    """
    Parse an S3 path into bucket and key components for AWS SDK usage.
    
    Args:
        s3_path (str): S3 path in format 'folder1/file.csv' or '/folder1/file.csv'
    
    Returns:
        tuple: (bucket_name, key) where bucket is from config and key is the path
    """
    return S3_BUCKET_NAME, s3_path.lstrip("/")
