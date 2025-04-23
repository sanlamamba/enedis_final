"""
Cloud Storage utilities for the Enedis project.

This module provides functions for interacting with Google Cloud Storage,
including reading from and writing to buckets, and efficient file handling.
"""

import os
import tempfile
import logging
from typing import Optional, Union
from pathlib import Path

from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError

from utils import retry
from config import CLOUD_PROCESSED_DIR


class CloudStorageClient:
    """A singleton class to manage Google Cloud Storage client instances."""

    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CloudStorageClient, cls).__new__(cls)
            cls._client = storage.Client()
        return cls._instance

    @property
    def client(self):
        return self._client


def get_storage_client() -> storage.Client:
    """
    Initialize and return a Google Cloud Storage client instance.

    Returns:
        google.cloud.storage.client.Client: An instance of the GCS client.
    """
    return CloudStorageClient().client


@retry(max_attempts=3, exceptions=(GoogleAPIError,))
def read_text_from_cloud(bucket_name: str, blob_name: str) -> str:
    """
    Download a blob from the specified Cloud Storage bucket as text.

    This function trims whitespace from the bucket name and downloads the blob's content
    entirely into memory. For very large files, consider using 'download_blob_to_temp_file'
    to avoid high memory consumption.

    Args:
        bucket_name: The name of the Cloud Storage bucket.
        blob_name: The path (or blob name) to the file within the bucket.

    Returns:
        The text content of the downloaded blob.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the download fails.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding="utf-8")
    except GoogleAPIError as e:
        logging.error(f"Failed to read text from {bucket_name}/{blob_name}: {e}")
        raise


@retry(max_attempts=3, exceptions=(GoogleAPIError,))
def download_blob_to_temp_file(bucket_name: str, blob_name: str) -> str:
    """
    Download a blob from Cloud Storage to a temporary file and return its file path.

    Args:
        bucket_name: The name of the Cloud Storage bucket.
        blob_name: The path (or blob name) to the file within the bucket.

    Returns:
        The file path to the downloaded temporary file.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the download fails.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        blob.download_to_filename(temp_file.name)
        logging.debug(f"Downloaded {blob_name} to temporary file {temp_file.name}")
        return temp_file.name
    except GoogleAPIError as e:
        logging.error(f"Failed to download {bucket_name}/{blob_name}: {e}")
        raise


@retry(max_attempts=3, exceptions=(GoogleAPIError,))
def upload_file_to_cloud(
    file_path: Union[str, Path],
    bucket_name: str,
    destination_blob_name: str,
    content_type: Optional[str] = None,
) -> str:
    """
    Upload a local file to a specified destination in Cloud Storage.

    Args:
        file_path: The local file path to the file that will be uploaded.
        bucket_name: The name of the Cloud Storage bucket.
        destination_blob_name: The destination path (or blob name) in the bucket.
        content_type: Optional content type to set for the uploaded file.

    Returns:
        The full GCS path to the uploaded file.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the upload fails.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()

    try:
        bucket = client.bucket(bucket_name)
        output_path = f"{CLOUD_PROCESSED_DIR}/{destination_blob_name}"
        blob = bucket.blob(output_path)

        if content_type:
            blob.content_type = content_type

        blob.upload_from_filename(file_path)
        logging.info(f"Uploaded {file_path} to gs://{bucket_name}/{output_path}")
        return f"gs://{bucket_name}/{output_path}"
    except GoogleAPIError as e:
        logging.error(
            f"Failed to upload {file_path} to {bucket_name}/{destination_blob_name}: {e}"
        )
        raise


def file_exists_in_cloud(bucket_name: str, blob_name: str) -> bool:
    """
    Check if a file exists in the specified Cloud Storage bucket.

    Args:
        bucket_name: The name of the Cloud Storage bucket.
        blob_name: The path (or blob name) to check in the bucket.

    Returns:
        True if the file exists, False otherwise.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logging.warning(f"Error checking if {blob_name} exists in {bucket_name}: {e}")
        return False


def list_blobs_with_prefix(bucket_name: str, prefix: str) -> list:
    """
    List blobs in a bucket with the given prefix.

    Args:
        bucket_name: The name of the Cloud Storage bucket.
        prefix: The prefix to filter blobs by.

    Returns:
        A list of blob names matching the prefix.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()

    try:
        bucket = client.bucket(bucket_name)
        blobs = client.list_blobs(bucket, prefix=prefix)
        return [blob.name for blob in blobs]
    except Exception as e:
        logging.error(f"Error listing blobs with prefix {prefix} in {bucket_name}: {e}")
        return []
