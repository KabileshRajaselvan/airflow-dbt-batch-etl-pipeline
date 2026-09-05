{#
  The PRD's own sketch for this test only ever produced a result set (a
  `null_count`/`duplicate_count` row) without failing anything - dbt generic
  tests must return zero rows to pass. Kept the PRD's two-check idea (null
  and duplicate on the same column) but wired it to dbt's actual pass/fail
  contract: return the offending column_name once per violation found.
#}
{% test not_null_unique(model, column_name) %}

with null_rows as (
    select 'null' as failure_type
    from {{ model }}
    where {{ column_name }} is null
),

duplicate_rows as (
    select 'duplicate' as failure_type
    from (
        select {{ column_name }}
        from {{ model }}
        group by {{ column_name }}
        having count(*) > 1
    ) dupes
)

select * from null_rows
union all
select * from duplicate_rows

{% endtest %}
