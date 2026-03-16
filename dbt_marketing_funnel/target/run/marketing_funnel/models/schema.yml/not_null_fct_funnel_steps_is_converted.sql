
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_converted
from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_marts`.`fct_funnel_steps`
where is_converted is null



  
  
      
    ) dbt_internal_test