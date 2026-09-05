"""Extraction from the MinIO (S3-compatible) file drop."""
import io
import os

import boto3
import pandas as pd


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def fetch_product_events(event_date: str, bucket: str | None = None, client=None) -> pd.DataFrame:
    bucket = bucket or os.environ.get("MINIO_BUCKET", "raw-files")
    client = client or _client()
    key = f"product_events/{event_date}.csv"
    obj = client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))
