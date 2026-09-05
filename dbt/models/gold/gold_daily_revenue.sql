select
    order_date as metric_date,
    count(*) as order_count,
    count(distinct user_id) as distinct_customers,
    sum(amount) as total_revenue,
    avg(amount) as avg_order_value
from {{ ref('silver_orders_cleaned') }}
where status = 'completed'
group by order_date
