import sys
from pathlib import Path

import pytest
from testcontainers.postgres import PostgresContainer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:15-alpine") as pg:
        yield pg


@pytest.fixture()
def client(postgres_container, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_DB_HOST", postgres_container.get_container_host_ip())
    monkeypatch.setenv("WAREHOUSE_DB_PORT", str(postgres_container.get_exposed_port(5432)))
    monkeypatch.setenv("WAREHOUSE_DB_NAME", postgres_container.dbname)
    monkeypatch.setenv("WAREHOUSE_DB_USER", postgres_container.username)
    monkeypatch.setenv("WAREHOUSE_DB_PASSWORD", postgres_container.password)

    from app.core.config import get_settings

    get_settings.cache_clear()

    from app.main import app

    from fastapi.testclient import TestClient

    return TestClient(app)


@pytest.fixture()
def seeded_marts(postgres_container, client):
    import psycopg2

    conn = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        dbname=postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS marts")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS marts.daily_revenue (
                metric_date DATE, order_count INT, distinct_customers INT,
                total_revenue NUMERIC, avg_order_value NUMERIC, published_at TIMESTAMPTZ
            )
            """
        )
        cur.execute("DELETE FROM marts.daily_revenue")
        cur.execute(
            """
            INSERT INTO marts.daily_revenue VALUES
            ('2026-01-01', 10, 8, 500.00, 50.00, now()),
            ('2026-01-02', 12, 9, 600.00, 50.00, now())
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS marts.daily_product_engagement (
                metric_date DATE, product_id TEXT, view_count INT,
                add_to_cart_count INT, purchase_count INT, distinct_users INT,
                published_at TIMESTAMPTZ
            )
            """
        )
        cur.execute("DELETE FROM marts.daily_product_engagement")
        cur.execute(
            """
            INSERT INTO marts.daily_product_engagement VALUES
            ('2026-01-01', 'product_1', 100, 20, 5, 60, now())
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS marts.user_ltv_segment_summary (
                segment TEXT, user_count INT, total_revenue NUMERIC,
                avg_ltv_score NUMERIC, published_at TIMESTAMPTZ
            )
            """
        )
        cur.execute("DELETE FROM marts.user_ltv_segment_summary")
        cur.execute(
            """
            INSERT INTO marts.user_ltv_segment_summary VALUES
            ('high_value', 200, 15000.00, 88.5, now())
            """
        )
    yield
    # The container is session-scoped (shared across tests for speed), so
    # tests that expect an "empty" state must not see another test's rows.
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA marts CASCADE")
    conn.close()
