
    
    

with dbt_test__target as (

  select session_id as unique_field
  from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_marts`.`fct_funnel_steps`
  where session_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


