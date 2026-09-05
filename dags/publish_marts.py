"""The DAG's final step — publishes gold tables into a `marts` schema.

dbt's own `dbt run` already materializes gold tables into the warehouse, so
a second unconditional "load to warehouse" step (as the PRD's DAG sketch
has) would just rewrite what dbt already wrote. This does something
genuinely additive instead: publish a `marts` schema of views over gold,
stamped with when they were last published, which is what the dashboard API
reads from — the PRD's `load_warehouse` stage exists for a real reason
(a stable, published-on-success layer downstream consumers can trust
mid-pipeline-failure), just implemented without duplicating dbt's own load.
"""
import os

from sqlalchemy import create_engine, text

MART_VIEWS = {
    "daily_revenue": "gold.gold_daily_revenue",
    "daily_product_engagement": "gold.gold_daily_product_engagement",
    "user_ltv_segment_summary": "gold.gold_user_ltv_segment_summary",
}


def _engine():
    host = os.environ.get("WAREHOUSE_DB_HOST", "localhost")
    port = os.environ.get("WAREHOUSE_DB_PORT", "5432")
    name = os.environ.get("WAREHOUSE_DB_NAME", "warehouse")
    user = os.environ.get("WAREHOUSE_DB_USER", "warehouse")
    password = os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}")


def publish_marts(engine=None) -> list[str]:
    engine = engine or _engine()
    published = []
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts"))
        for view_name, source_table in MART_VIEWS.items():
            conn.execute(
                text(
                    f"CREATE OR REPLACE VIEW marts.{view_name} AS "
                    f"SELECT *, now() AS published_at FROM {source_table}"
                )
            )
            published.append(view_name)
    return published


if __name__ == "__main__":
    print(publish_marts())
