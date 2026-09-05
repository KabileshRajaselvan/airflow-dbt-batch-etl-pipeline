import sys
from pathlib import Path

import boto3
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from extract_files import fetch_product_events


@mock_aws
def test_fetch_product_events_reads_csv_from_bucket():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="raw-files")
    csv_body = (
        "event_id,user_id,product_id,event_type,event_time,price\n"
        "e1,user_1,product_1,view,2026-01-01T00:00:00,19.99\n"
    )
    client.put_object(Bucket="raw-files", Key="product_events/2026-01-01.csv", Body=csv_body.encode())

    df = fetch_product_events("2026-01-01", bucket="raw-files", client=client)

    assert len(df) == 1
    assert df.iloc[0]["event_id"] == "e1"
    assert df.iloc[0]["product_id"] == "product_1"
