
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        deepest_stage_name as value_field,
        count(*) as n_records

    from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_marts`.`fct_funnel_steps`
    group by deepest_stage_name

)

select *
from all_values
where value_field not in (
    'session_start','page_view','view_item','add_to_cart','begin_checkout','add_payment_info','purchase'
)



  
  
      
    ) dbt_internal_test