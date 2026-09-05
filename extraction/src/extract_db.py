"""Extraction from the source Postgres (simulates a production OLTP DB)."""
import os

import pandas as pd
import psycopg2


def _connection_params() -> dict:
    return {
        "host": os.environ.get("SOURCE_DB_HOST", "localhost"),
        "port": int(os.environ.get("SOURCE_DB_PORT", "5432")),
        "dbname": os.environ.get("SOURCE_DB_NAME", "sourcedb"),
        "user": os.environ.get("SOURCE_DB_USER", "source"),
        "password": os.environ.get("SOURCE_DB_PASSWORD", "source"),
    }


def fetch_orders(order_date: str, conn_params: dict | None = None) -> pd.DataFrame:
    conn_params = conn_params or _connection_params()
    with psycopg2.connect(**conn_params) as conn:
        return pd.read_sql(
            "SELECT order_id, user_id, order_date, amount, status, created_at "
            "FROM orders WHERE order_date = %(order_date)s",
            conn,
            params={"order_date": order_date},
        )
