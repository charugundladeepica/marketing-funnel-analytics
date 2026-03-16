
    
    

with all_values as (

    select
        had_purchase as value_field,
        count(*) as n_records

    from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_staging`.`stg_ga4_sessions`
    group by had_purchase

)

select *
from all_values
where value_field not in (
    'True','False'
)


