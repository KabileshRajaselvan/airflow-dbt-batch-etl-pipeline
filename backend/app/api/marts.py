import pandas as pd
from fastapi import APIRouter, Query
from sqlalchemy import text

from app.core.db import get_engine
from app.schemas.responses import (
    DailyRevenueOut,
    PipelineStatusOut,
    ProductEngagementOut,
    SegmentSummaryOut,
)

router = APIRouter(prefix="/api/marts", tags=["marts"])


def _read_mart(view: str, limit: int, order_by: str = "metric_date") -> pd.DataFrame:
    engine = get_engine()
    try:
        return pd.read_sql(
            text(f"SELECT * FROM marts.{view} ORDER BY {order_by} DESC LIMIT :limit"),
            engine,
            params={"limit": limit},
        )
    except Exception:
        return pd.DataFrame()


@router.get("/daily-revenue", response_model=list[DailyRevenueOut])
def daily_revenue(limit: int = Query(30, ge=1, le=365)) -> list[dict]:
    df = _read_mart("daily_revenue", limit)
    return df.to_dict(orient="records") if not df.empty else []


@router.get("/product-engagement", response_model=list[ProductEngagementOut])
def product_engagement(limit: int = Query(50, ge=1, le=1000)) -> list[dict]:
    df = _read_mart("daily_product_engagement", limit, order_by="view_count")
    return df.to_dict(orient="records") if not df.empty else []


@router.get("/segment-summary", response_model=list[SegmentSummaryOut])
def segment_summary() -> list[dict]:
    df = _read_mart("user_ltv_segment_summary", limit=100, order_by="total_revenue")
    return df.to_dict(orient="records") if not df.empty else []


@router.get("/status", response_model=PipelineStatusOut)
def pipeline_status() -> dict:
    df = _read_mart("daily_revenue", limit=10000)
    if df.empty:
        return {"latest_metric_date": None, "days_available": 0, "latest_published_at": None}
    return {
        "latest_metric_date": df["metric_date"].max(),
        "days_available": df["metric_date"].nunique(),
        "latest_published_at": df["published_at"].max(),
    }
