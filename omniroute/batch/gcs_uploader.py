"""
Thin wrapper around google-cloud-storage.

On the GCP VM, the attached service account's IAM role is used
automatically via Application Default Credentials — no key file
or env var is needed as long as the VM's service account has
storage.objects.create (e.g. roles/storage.objectAdmin) on the bucket.
"""

import os

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

_client = None


def get_client():
    global _client
    if _client is None:
        from google.cloud import storage  # imported lazily so --no-upload works without the package
        _client = storage.Client()  # uses VM's attached service account
    return _client


def upload_file(local_path: str, blob_path: str, bucket_name: str = None) -> str:
    """
    Upload a local file to gs://{bucket_name}/{blob_path}.
    Returns the gs:// URI of the uploaded object.
    """
    bucket_name = bucket_name or GCS_BUCKET_NAME
    if not bucket_name:
        raise RuntimeError(
            "No GCS bucket configured. Set GCS_BUCKET_NAME env var "
            "or pass bucket_name explicitly."
        )

    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

    uri = f"gs://{bucket_name}/{blob_path}"
    print(f"Uploaded {local_path} -> {uri}")
    return uri
