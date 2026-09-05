from datetime import date, datetime

from pydantic import BaseModel


class DailyRevenueOut(BaseModel):
    metric_date: date
    order_count: int
    distinct_customers: int
    total_revenue: float
    avg_order_value: float
    published_at: datetime


class ProductEngagementOut(BaseModel):
    metric_date: date
    product_id: str
    view_count: int
    add_to_cart_count: int
    purchase_count: int
    distinct_users: int
    published_at: datetime


class SegmentSummaryOut(BaseModel):
    segment: str
    user_count: int
    total_revenue: float
    avg_ltv_score: float
    published_at: datetime


class PipelineStatusOut(BaseModel):
    latest_metric_date: date | None
    days_available: int
    latest_published_at: datetime | None
