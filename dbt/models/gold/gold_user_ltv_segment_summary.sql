select
    e.segment,
    count(distinct e.user_id) as user_count,
    coalesce(sum(o.amount), 0) as total_revenue,
    avg(e.ltv_score) as avg_ltv_score
from {{ ref('silver_user_enrichment_cleaned') }} e
left join {{ ref('silver_orders_cleaned') }} o
    on e.user_id = o.user_id and o.status = 'completed'
group by e.segment
