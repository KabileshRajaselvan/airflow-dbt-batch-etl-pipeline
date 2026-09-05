"""Uploads synthetic `product_events` CSV files to MinIO (S3-compatible) —
stands in for a daily file drop from an upstream system landing in S3.
"""
import io
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone

import boto3
import pandas as pd

DAYS_OF_HISTORY = int(os.environ.get("SEED_DAYS", "14"))
EVENTS_PER_DAY = int(os.environ.get("SEED_EVENTS_PER_DAY", "5000"))
NUM_USERS = 2000
NUM_PRODUCTS = 300
EVENT_TYPES = ["view", "view", "view", "add_to_cart", "purchase"]
BUCKET = os.environ.get("MINIO_BUCKET", "raw-files")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def _events_for_day(rng: random.Random, event_date: date) -> pd.DataFrame:
    rows = []
    for _ in range(EVENTS_PER_DAY):
        event_time = datetime.combine(event_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
            seconds=rng.randint(0, 86399)
        )
        rows.append(
            {
                "event_id": str(uuid.uuid4()),
                "user_id": f"user_{rng.randint(1, NUM_USERS)}",
                "product_id": f"product_{rng.randint(1, NUM_PRODUCTS)}",
                "event_type": rng.choice(EVENT_TYPES),
                "event_time": event_time.isoformat(),
                "price": round(rng.uniform(5.0, 300.0), 2),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    client = get_client()

    existing_buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if BUCKET not in existing_buckets:
        client.create_bucket(Bucket=BUCKET)

    rng = random.Random(43)
    today = date.today()
    uploaded = 0
    for day_offset in range(DAYS_OF_HISTORY):
        event_date = today - timedelta(days=day_offset)
        key = f"product_events/{event_date.isoformat()}.csv"

        existing = client.list_objects_v2(Bucket=BUCKET, Prefix=key)
        if existing.get("KeyCount", 0) > 0:
            continue

        df = _events_for_day(rng, event_date)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        client.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"))
        uploaded += 1

    print(f"uploaded {uploaded} daily product_events files to s3://{BUCKET}/product_events/")


if __name__ == "__main__":
    main()
