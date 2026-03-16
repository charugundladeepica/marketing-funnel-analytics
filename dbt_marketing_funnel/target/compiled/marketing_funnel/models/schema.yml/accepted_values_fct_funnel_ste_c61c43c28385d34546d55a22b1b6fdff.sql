
    
    

with all_values as (

    select
        max_funnel_stage as value_field,
        count(*) as n_records

    from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_marts`.`fct_funnel_steps`
    group by max_funnel_stage

)

select *
from all_values
where value_field not in (
    1,2,3,4,5,6,7
)


