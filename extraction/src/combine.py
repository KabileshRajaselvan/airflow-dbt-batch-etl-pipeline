"""PySpark job: type/validate each extracted source and load it into its own
`raw.*` table in the warehouse via JDBC.

Each source keeps its own raw table rather than being naively unioned (as the
PRD's own `extract.py` sketch does via `spark.union([api_data, db_data,
s3_data])`) — user-enrichment, orders, and product-events have genuinely
different schemas, and forcing a union would mean picking a lowest-common-
denominator schema that throws away most of each source's columns. dbt's
staging/silver/gold models are what actually combine them, via real joins.
"""
import logging
import os
import sys
from decimal import Decimal

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from schema import ORDERS_SCHEMA, PRODUCT_EVENTS_SCHEMA, USER_ENRICHMENT_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("combine")


def _to_decimal(value) -> Decimal:
    # spark.createDataFrame(pdf, schema=...) with an explicit DecimalType
    # field rejects a plain python float ("CANNOT_ACCEPT_OBJECT_IN_TYPE") when
    # not using Arrow - it needs a real decimal.Decimal instance per cell.
    # str(value) first avoids Decimal's binary-float representation artifacts
    # (Decimal(90.0) can render as 90.000000000000014...).
    return Decimal(str(value))

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

# Windows-local-mode PySpark needs the worker interpreter pinned explicitly,
# or the driver spawns whatever bare "python"/"python3" resolves to on PATH -
# on this machine that's the Windows Store's python.exe stub, which prints
# "Python was not found..." and never connects back, timing out the driver's
# accept() with SocketTimeoutException. Doesn't apply inside the Docker image
# (a real python is always first on PATH there), but matches the same fix
# applied in streaming/tests/conftest.py for the sibling streaming project.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def build_spark(app_name: str = "etl-combine") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "4"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def _warehouse_jdbc_url() -> str:
    host = os.environ.get("WAREHOUSE_DB_HOST", "localhost")
    port = os.environ.get("WAREHOUSE_DB_PORT", "5432")
    name = os.environ.get("WAREHOUSE_DB_NAME", "warehouse")
    return f"jdbc:postgresql://{host}:{port}/{name}"


def _warehouse_jdbc_properties() -> dict:
    return {
        "user": os.environ.get("WAREHOUSE_DB_USER", "warehouse"),
        "password": os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse"),
        "driver": "org.postgresql.Driver",
    }


def _delete_existing(table: str, date_column: str, run_date: str) -> None:
    """Idempotency for DAG re-runs: clear this run_date's rows before the
    JDBC append, using the same psycopg2 connection the extraction modules
    already use rather than a second Spark round-trip."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("WAREHOUSE_DB_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_DB_PORT", "5432")),
        dbname=os.environ.get("WAREHOUSE_DB_NAME", "warehouse"),
        user=os.environ.get("WAREHOUSE_DB_USER", "warehouse"),
        password=os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse"),
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
        # table/column names here are fixed constants from this module's own
        # call sites (never user input); Spark's JDBC writer creates the
        # table itself on first run (see load_source_to_raw), so this only
        # ever runs against a table that already exists.
        cur.execute(f"DELETE FROM {table} WHERE {date_column} = %s", (run_date,))  # noqa: S608
    conn.close()


def load_source_to_raw(
    pdf: pd.DataFrame,
    spark: SparkSession,
    schema,
    table: str,
    date_column: str,
    run_date: str,
) -> int:
    if pdf.empty:
        logger.warning("no rows extracted for %s on %s", table, run_date)
        return 0

    # spark.createDataFrame(pandas_df, schema=explicit_schema) zips pandas
    # columns against the schema's fields POSITIONALLY, not by name - it
    # silently mismatches types if a column got added out of schema order
    # (e.g. event_date, appended at the end of the DataFrame after the
    # normalization step above, needs to land in its schema-declared slot
    # between event_time and price, not after price).
    pdf = pdf[schema.fieldNames()]
    sdf: DataFrame = spark.createDataFrame(pdf, schema=schema)

    try:
        _delete_existing(table, date_column, run_date)
    except Exception:
        logger.info("raw.%s not created yet (first run) - skipping delete", table)

    (
        sdf.write.jdbc(
            url=_warehouse_jdbc_url(),
            table=table,
            mode="append",
            properties=_warehouse_jdbc_properties(),
        )
    )
    count = sdf.count()
    logger.info("loaded %d rows into %s for %s", count, table, run_date)
    return count


def combine_and_load(
    user_enrichment_pdf: pd.DataFrame,
    orders_pdf: pd.DataFrame,
    product_events_pdf: pd.DataFrame,
    run_date: str,
) -> dict:
    # Normalize source-native string/tz-aware datetime columns to the plain
    # python date/datetime objects Spark's createDataFrame needs to match an
    # explicit schema (a raw ISO string in a DateType column isn't reliably
    # auto-cast, and psycopg2 returns tz-aware datetimes for TIMESTAMPTZ that
    # are simplest to normalize to naive UTC before handing to Spark).
    if not user_enrichment_pdf.empty:
        user_enrichment_pdf = user_enrichment_pdf.copy()
        user_enrichment_pdf["as_of_date"] = pd.to_datetime(user_enrichment_pdf["as_of_date"]).dt.date
        user_enrichment_pdf["extracted_at"] = pd.to_datetime(user_enrichment_pdf["extracted_at"]).dt.tz_localize(None)
        user_enrichment_pdf["ltv_score"] = user_enrichment_pdf["ltv_score"].apply(_to_decimal)

    if not orders_pdf.empty:
        orders_pdf = orders_pdf.copy()
        # order_date arrives as a real datetime.date from psycopg2/pd.read_sql
        # against a Postgres DATE column in production, but normalizing it
        # explicitly here (rather than trusting the source) also makes this
        # robust to a plain ISO string, e.g. from a test fixture or a
        # different DB driver.
        orders_pdf["order_date"] = pd.to_datetime(orders_pdf["order_date"]).dt.date
        orders_pdf["created_at"] = pd.to_datetime(orders_pdf["created_at"], utc=True).dt.tz_localize(None)
        orders_pdf["amount"] = orders_pdf["amount"].apply(_to_decimal)

    if not product_events_pdf.empty:
        product_events_pdf = product_events_pdf.copy()
        product_events_pdf["event_time"] = pd.to_datetime(product_events_pdf["event_time"], utc=True).dt.tz_localize(
            None
        )
        product_events_pdf["event_date"] = product_events_pdf["event_time"].dt.date
        product_events_pdf["price"] = product_events_pdf["price"].apply(_to_decimal)

    spark = build_spark()
    try:
        counts = {
            "user_enrichment": load_source_to_raw(
                user_enrichment_pdf, spark, USER_ENRICHMENT_SCHEMA, "raw.user_enrichment", "as_of_date", run_date
            ),
            "orders": load_source_to_raw(
                orders_pdf, spark, ORDERS_SCHEMA, "raw.orders", "order_date", run_date
            ),
            "product_events": load_source_to_raw(
                product_events_pdf, spark, PRODUCT_EVENTS_SCHEMA, "raw.product_events", "event_date", run_date
            ),
        }
        return counts
    finally:
        spark.stop()
