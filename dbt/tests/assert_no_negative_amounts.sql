-- Singular test: gold_daily_revenue should never show negative revenue for
-- a day (silver_orders_cleaned already filters amount >= 0, but this checks
-- the aggregate itself as a final sanity gate on the actual gold table).
select metric_date, total_revenue
from {{ ref('gold_daily_revenue') }}
where total_revenue < 0
