

  create or replace view `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_staging`.`stg_ga4_sessions`
  OPTIONS()
  as 

/*
  Aggregates stg_ga4_events to session grain.
  - First-touch attribution: takes the source/medium/campaign from the
    first event in the session (lowest event_timestamp).
  - Rolls up event counts per session to make funnel logic simple downstream.
*/

with

events as (

    select * from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_staging`.`stg_ga4_events`

),

session_attribution as (

    -- First-touch: pick source/medium from the earliest event in the session
    select distinct
        session_id,
        first_value(traffic_source)   over w  as attributed_source,
        first_value(traffic_medium)   over w  as attributed_medium,
        first_value(campaign_name)    over w  as attributed_campaign,
        first_value(device_category)  over w  as device_category,
        first_value(country)          over w  as country,
        first_value(event_date)       over w  as session_date

    from events

    window w as (
        partition by session_id
        order by event_timestamp asc
        rows between unbounded preceding and unbounded following
    )

),

session_events as (

    select
        session_id,
        user_pseudo_id,
        min(event_timestamp)                                                    as session_start_at,
        max(event_timestamp)                                                    as session_end_at,

        -- Funnel step flags — TRUE if the session contains this event
        countif(event_name = 'session_start')         > 0                      as had_session_start,
        countif(event_name = 'page_view')             > 0                      as had_page_view,
        countif(event_name = 'view_item')             > 0                      as had_view_item,
        countif(event_name = 'add_to_cart')           > 0                      as had_add_to_cart,
        countif(event_name = 'begin_checkout')        > 0                      as had_begin_checkout,
        countif(event_name = 'add_payment_info')      > 0                      as had_add_payment_info,
        countif(event_name = 'purchase')              > 0                      as had_purchase,

        -- Revenue (null for non-converting sessions)
        sum(purchase_revenue)                                                   as session_revenue,
        max(transaction_id)                                                     as transaction_id,

        -- Engagement
        max(case when session_engaged = '1' then true else false end)           as is_engaged_session,
        count(*)                                                                as total_events

    from events
    group by session_id, user_pseudo_id

)

select
    se.session_id,
    se.user_pseudo_id,
    sa.session_date,
    se.session_start_at,
    se.session_end_at,
    timestamp_diff(se.session_end_at, se.session_start_at, second)  as session_duration_seconds,

    -- Attribution
    sa.attributed_source,
    sa.attributed_medium,
    sa.attributed_campaign,
    sa.device_category,
    sa.country,

    -- Funnel flags
    se.had_session_start,
    se.had_page_view,
    se.had_view_item,
    se.had_add_to_cart,
    se.had_begin_checkout,
    se.had_add_payment_info,
    se.had_purchase,

    -- Revenue & engagement
    coalesce(se.session_revenue, 0)     as session_revenue,
    se.transaction_id,
    se.is_engaged_session,
    se.total_events

from session_events se
left join session_attribution sa
    on se.session_id = sa.session_id;

