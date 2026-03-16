
    
    

with all_values as (

    select
        channel_group as value_field,
        count(*) as n_records

    from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_marts`.`fct_funnel_steps`
    group by channel_group

)

select *
from all_values
where value_field not in (
    'Paid Search','Paid Social','Email','Organic Search','Referral','Direct','Organic Social','Other'
)


