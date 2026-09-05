"""Airflow entrypoint: pull the day's gold tables from the warehouse and run
the DataQualityFramework against each. Raises DataQualityError (failing the
Airflow task) on any violation.
"""
import os

import pandas as pd
from sqlalchemy import create_engine

from data_quality import DataQualityFramework, QualityThresholds


def _engine():
    host = os.environ.get("WAREHOUSE_DB_HOST", "localhost")
    port = os.environ.get("WAREHOUSE_DB_PORT", "5432")
    name = os.environ.get("WAREHOUSE_DB_NAME", "warehouse")
    user = os.environ.get("WAREHOUSE_DB_USER", "warehouse")
    password = os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}")


def run_all_checks(run_date: str, engine=None) -> dict:
    engine = engine or _engine()
    results = {}

    revenue_df = pd.read_sql(
        "SELECT * FROM gold.gold_daily_revenue WHERE metric_date = %(d)s", engine, params={"d": run_date}
    )
    results["daily_revenue"] = DataQualityFramework.run_checks(
        revenue_df,
        required_columns=["metric_date", "order_count", "total_revenue", "avg_order_value"],
        unique_column="metric_date",
        thresholds=QualityThresholds(min_row_count=1, value_range_column="total_revenue", date_column="metric_date"),
    )

    engagement_df = pd.read_sql(
        "SELECT * FROM gold.gold_daily_product_engagement WHERE metric_date = %(d)s",
        engine,
        params={"d": run_date},
    )
    results["daily_product_engagement"] = DataQualityFramework.run_checks(
        engagement_df,
        required_columns=["metric_date", "product_id", "view_count", "distinct_users"],
        unique_column="product_id",
        thresholds=QualityThresholds(
            min_row_count=1, value_range_column="view_count", date_column="metric_date"
        ),
    )

    return results


if __name__ == "__main__":
    import sys
    from datetime import date

    run_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
    outcome = run_all_checks(run_date)
    print(outcome)
