"""Seeds the `source-db` Postgres with a synthetic `orders` table — stands in
for a production OLTP database the ETL pipeline extracts from daily.
"""
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone

import psycopg2

NUM_USERS = 2000
DAYS_OF_HISTORY = int(os.environ.get("SEED_DAYS", "14"))
ORDERS_PER_DAY = int(os.environ.get("SEED_ORDERS_PER_DAY", "3000"))
STATUSES = ["completed", "completed", "completed", "refunded", "pending"]


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("SOURCE_DB_HOST", "localhost"),
        port=int(os.environ.get("SOURCE_DB_PORT", "5432")),
        dbname=os.environ.get("SOURCE_DB_NAME", "sourcedb"),
        user=os.environ.get("SOURCE_DB_USER", "source"),
        password=os.environ.get("SOURCE_DB_PASSWORD", "source"),
    )


def main() -> None:
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            user_id TEXT NOT NULL,
            order_date DATE NOT NULL,
            amount NUMERIC(10, 2) NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    cur.execute("SELECT COUNT(*) FROM orders")
    (existing_count,) = cur.fetchone()
    if existing_count > 0:
        print(f"orders already seeded ({existing_count} rows) - skipping")
        cur.close()
        conn.close()
        return

    rng = random.Random(42)
    today = date.today()
    rows = []
    for day_offset in range(DAYS_OF_HISTORY):
        order_date = today - timedelta(days=day_offset)
        for _ in range(ORDERS_PER_DAY):
            created_at = datetime.combine(order_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
                seconds=rng.randint(0, 86399)
            )
            rows.append(
                (
                    str(uuid.uuid4()),
                    f"user_{rng.randint(1, NUM_USERS)}",
                    order_date,
                    round(rng.uniform(5.0, 500.0), 2),
                    rng.choice(STATUSES),
                    created_at,
                )
            )

    cur.executemany(
        """
        INSERT INTO orders (order_id, user_id, order_date, amount, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    print(f"seeded {len(rows)} orders across {DAYS_OF_HISTORY} days")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
