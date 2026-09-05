from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

USER_ENRICHMENT_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("segment", StringType(), nullable=False),
        StructField("ltv_score", DecimalType(6, 2), nullable=False),
        StructField("acquisition_channel", StringType(), nullable=False),
        StructField("as_of_date", DateType(), nullable=False),
        StructField("extracted_at", TimestampType(), nullable=False),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("order_date", DateType(), nullable=False),
        StructField("amount", DecimalType(10, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
    ]
)

PRODUCT_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("price", DecimalType(10, 2), nullable=False),
    ]
)
