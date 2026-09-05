"""Seeds the `raw` schema with synthetic data for CI's dbt job — a
lightweight stand-in for the real `combine_and_load` PySpark step (which is
already covered for real by extraction/tests/test_combine_integration.py)
so the dbt job doesn't need a JDK/PySpark just to get rows into `raw`.
"""
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone

import psycopg2

conn = psycopg2.connect(
    host=os.environ.get("WAREHOUSE_DB_HOST", "localhost"),
    port=int(os.environ.get("WAREHOUSE_DB_PORT", "5432")),
    dbname=os.environ.get("WAREHOUSE_DB_NAME", "warehouse"),
    user=os.environ.get("WAREHOUSE_DB_USER", "warehouse"),
    password=os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse"),
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE SCHEMA IF NOT EXISTS raw")

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS raw.user_enrichment (
        user_id TEXT, segment TEXT, ltv_score NUMERIC(6,2), acquisition_channel TEXT,
        as_of_date DATE, extracted_at TIMESTAMP
    )
    """
)
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS raw.orders (
        order_id TEXT, user_id TEXT, order_date DATE, amount NUMERIC(10,2), status TEXT, created_at TIMESTAMP
    )
    """
)
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS raw.product_events (
        event_id TEXT, user_id TEXT, product_id TEXT, event_type TEXT,
        event_time TIMESTAMP, event_date DATE, price NUMERIC(10,2)
    )
    """
)

rng = random.Random(1)
today = date.today()
SEGMENTS = ["high_value", "standard", "at_risk", "new"]
CHANNELS = ["organic", "paid_search", "paid_social", "referral", "email"]
STATUSES = ["completed", "completed", "refunded", "pending"]
EVENT_TYPES = ["view", "view", "add_to_cart", "purchase"]

NUM_USERS = 500

enrichment_rows = [
    (f"user_{i}", rng.choice(SEGMENTS), round(rng.uniform(0, 100), 2), rng.choice(CHANNELS), today, datetime.now(timezone.utc))
    for i in range(1, NUM_USERS + 1)
]
cur.executemany("INSERT INTO raw.user_enrichment VALUES (%s,%s,%s,%s,%s,%s)", enrichment_rows)

order_rows = [
    (str(uuid.uuid4()), f"user_{rng.randint(1, NUM_USERS)}", today, round(rng.uniform(5, 500), 2), rng.choice(STATUSES), datetime.now(timezone.utc))
    for _ in range(2000)
]
cur.executemany("INSERT INTO raw.orders VALUES (%s,%s,%s,%s,%s,%s)", order_rows)

event_rows = []
for _ in range(3000):
    et = datetime.now(timezone.utc) - timedelta(seconds=rng.randint(0, 86399))
    event_rows.append(
        (str(uuid.uuid4()), f"user_{rng.randint(1, NUM_USERS)}", f"product_{rng.randint(1, 100)}", rng.choice(EVENT_TYPES), et, today, round(rng.uniform(5, 300), 2))
    )
cur.executemany("INSERT INTO raw.product_events VALUES (%s,%s,%s,%s,%s,%s,%s)", event_rows)

print(f"seeded {len(enrichment_rows)} enrichment, {len(order_rows)} orders, {len(event_rows)} events")
cur.close()
conn.close()
