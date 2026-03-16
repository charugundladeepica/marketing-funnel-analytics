
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_revenue
from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_staging`.`stg_ga4_sessions`
where session_revenue is null



  
  
      
    ) dbt_internal_test