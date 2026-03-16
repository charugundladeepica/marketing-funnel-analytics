
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select deepest_stage_name
from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_marts`.`fct_funnel_steps`
where deepest_stage_name is null



  
  
      
    ) dbt_internal_test