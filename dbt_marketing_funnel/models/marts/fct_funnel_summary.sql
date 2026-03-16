{{
    config(
        materialized='table',
        partition_by={
            'field': 'session_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        description="""
            Pre-aggregated funnel metrics by date + channel + device.
            Use this model in Power BI / Streamlit for fast dashboard queries.
            Avoids scanning the full fct_funnel_steps table on every visual refresh.
        """
    )
}}

with

funnel as (

    select * from {{ ref('fct_funnel_steps') }}

)

select
    session_date,
    channel_group,
    attributed_source,
    attributed_medium,
    attributed_campaign,
    device_category,
    country,

    -- Volume at each funnel stage
    count(*)                                                    as total_sessions,
    countif(had_page_view)                                      as sessions_with_page_view,
    countif(had_view_item)                                      as sessions_with_view_item,
    countif(had_add_to_cart)                                    as sessions_with_add_to_cart,
    countif(had_begin_checkout)                                 as sessions_with_checkout,
    countif(had_add_payment_info)                               as sessions_with_payment,
    countif(had_purchase)                                       as sessions_with_purchase,

    -- Step-to-step conversion rates (as FLOAT for BI tools)
    safe_divide(countif(had_page_view),         count(*))       as rate_session_to_page_view,
    safe_divide(countif(had_view_item),         countif(had_page_view))      as rate_page_view_to_view_item,
    safe_divide(countif(had_add_to_cart),       countif(had_view_item))      as rate_view_item_to_add_to_cart,
    safe_divide(countif(had_begin_checkout),    countif(had_add_to_cart))    as rate_cart_to_checkout,
    safe_divide(countif(had_add_payment_info),  countif(had_begin_checkout)) as rate_checkout_to_payment,
    safe_divide(countif(had_purchase),          countif(had_add_payment_info)) as rate_payment_to_purchase,

    -- Overall session-to-purchase rate
    safe_divide(countif(had_purchase), count(*))                as overall_conversion_rate,

    -- Revenue
    sum(session_revenue)                                        as total_revenue,
    safe_divide(sum(session_revenue), countif(had_purchase))    as avg_order_value,

    -- Engagement
    countif(is_engaged_session)                                 as engaged_sessions,
    safe_divide(countif(is_engaged_session), count(*))          as engagement_rate,

    avg(session_duration_seconds)                               as avg_session_duration_seconds

from funnel
group by
    session_date,
    channel_group,
    attributed_source,
    attributed_medium,
    attributed_campaign,
    device_category,
    country
