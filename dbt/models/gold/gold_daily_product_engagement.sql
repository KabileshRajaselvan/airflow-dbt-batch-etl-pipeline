select
    event_date as metric_date,
    product_id,
    count(*) filter (where event_type = 'view') as view_count,
    count(*) filter (where event_type = 'add_to_cart') as add_to_cart_count,
    count(*) filter (where event_type = 'purchase') as purchase_count,
    count(distinct user_id) as distinct_users
from {{ ref('silver_product_events_cleaned') }}
group by event_date, product_id
