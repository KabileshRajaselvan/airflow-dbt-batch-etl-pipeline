"""Real Postgres (via testcontainers) integration test for extract_db —
psycopg2's named-parameter style (`%(name)s`) doesn't translate cleanly onto
an in-memory sqlite stand-in, so this uses a real, disposable Postgres
instead of mocking the DB-API layer.
"""
import sys
from datetime import date
from pathlib import Path

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from extract_db import fetch_orders

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:15-alpine") as pg:
        yield pg


@pytest.fixture()
def conn_params(postgres_container):
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": postgres_container.get_exposed_port(5432),
        "dbname": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
    }


def test_fetch_orders_returns_only_requested_date(conn_params):
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE orders (
                order_id TEXT, user_id TEXT, order_date DATE,
                amount NUMERIC, status TEXT, created_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            "INSERT INTO orders VALUES "
            "('o1', 'user_1', '2026-01-01', 50.0, 'completed', '2026-01-01T10:00:00Z'), "
            "('o2', 'user_2', '2026-01-02', 75.0, 'completed', '2026-01-02T10:00:00Z')"
        )
    conn.close()

    df = fetch_orders("2026-01-01", conn_params=conn_params)

    assert len(df) == 1
    assert df.iloc[0]["order_id"] == "o1"
    assert df.iloc[0]["order_date"] == date(2026, 1, 1)
