"""Real Postgres (via testcontainers) + real PySpark JDBC write, verifying
combine.py's load-and-idempotent-rerun behavior end-to-end.
"""
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import combine  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:15-alpine") as pg:
        yield pg


@pytest.fixture()
def warehouse_env(postgres_container, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_DB_HOST", postgres_container.get_container_host_ip())
    monkeypatch.setenv("WAREHOUSE_DB_PORT", str(postgres_container.get_exposed_port(5432)))
    monkeypatch.setenv("WAREHOUSE_DB_NAME", postgres_container.dbname)
    monkeypatch.setenv("WAREHOUSE_DB_USER", postgres_container.username)
    monkeypatch.setenv("WAREHOUSE_DB_PASSWORD", postgres_container.password)
    return postgres_container


def _sample_frames():
    user_enrichment = pd.DataFrame(
        [{"user_id": "user_1", "segment": "high_value", "ltv_score": 90.0,
          "acquisition_channel": "organic", "as_of_date": "2026-01-01",
          "extracted_at": "2026-01-01T00:00:00+00:00"}]
    )
    orders = pd.DataFrame(
        [{"order_id": "o1", "user_id": "user_1", "order_date": "2026-01-01",
          "amount": 50.0, "status": "completed", "created_at": "2026-01-01T10:00:00+00:00"}]
    )
    product_events = pd.DataFrame(
        [{"event_id": "e1", "user_id": "user_1", "product_id": "product_1",
          "event_type": "view", "event_time": "2026-01-01T00:00:00", "price": 9.99}]
    )
    return user_enrichment, orders, product_events


def test_combine_and_load_is_idempotent_on_rerun(warehouse_env):
    user_enrichment, orders, product_events = _sample_frames()

    combine.combine_and_load(user_enrichment, orders, product_events, run_date="2026-01-01")
    combine.combine_and_load(user_enrichment, orders, product_events, run_date="2026-01-01")

    conn = psycopg2.connect(
        host=warehouse_env.get_container_host_ip(),
        port=warehouse_env.get_exposed_port(5432),
        dbname=warehouse_env.dbname,
        user=warehouse_env.username,
        password=warehouse_env.password,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.orders WHERE order_date = '2026-01-01'")
        (order_count,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM raw.user_enrichment WHERE as_of_date = '2026-01-01'")
        (enrichment_count,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM raw.product_events WHERE event_date = '2026-01-01'")
        (events_count,) = cur.fetchone()
    conn.close()

    assert order_count == 1
    assert enrichment_count == 1
    assert events_count == 1
