# cloud_storage_utils.py
from google.cloud import storage
import tempfile
from config import CLOUD_PROCESSED_DIR


def get_storage_client():
    """
    Initialize and return a Google Cloud Storage client instance.

    Returns:
        google.cloud.storage.client.Client: An instance of the GCS client.
    """
    return storage.Client()


def read_text_from_cloud(bucket_name, blob_name):
    """
    Download a blob from the specified Cloud Storage bucket as text.

    This function trims whitespace from the bucket name and downloads the blob's content
    entirely into memory. For very large files, consider using 'download_blob_to_temp_file'
    to avoid high memory consumption.

    Args:
        bucket_name (str): The name of the Cloud Storage bucket.
        blob_name (str): The path (or blob name) to the file within the bucket.

    Returns:
        str: The text content of the downloaded blob.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the download fails.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text(encoding="utf-8")


def download_blob_to_temp_file(bucket_name, blob_name):
    """
    Download a blob from Cloud Storage to a temporary file and return its file path.

    Args:
        bucket_name (str): The name of the Cloud Storage bucket.
        blob_name (str): The path (or blob name) to the file within the bucket.

    Returns:
        str: The file path to the downloaded temporary file.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the download fails.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    blob.download_to_filename(temp_file.name)
    return temp_file.name


def upload_file_to_cloud(file_path, bucket_name, destination_blob_name):
    """
    Upload a local file to a specified destination in Cloud Storage.

    Args:
        file_path (str): The local file path to the file that will be uploaded.
        bucket_name (str): The name of the Cloud Storage bucket.
        destination_blob_name (str): The destination path (or blob name) in the bucket.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the upload fails.
    """
    client = get_storage_client()
    bucket_name = bucket_name.strip()
    bucket = client.bucket(bucket_name)
    output_path = CLOUD_PROCESSED_DIR + "/" + destination_blob_name
    blob = bucket.blob(output_path)
    blob.upload_from_filename(file_path)
