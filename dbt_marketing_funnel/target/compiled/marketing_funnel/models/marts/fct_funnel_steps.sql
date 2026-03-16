

with

sessions as (

    select * from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_staging`.`stg_ga4_sessions`

),

/*
  Assign each session a numeric funnel stage — the deepest step reached.
  This makes aggregation and drop-off calculation straightforward.

  Stage mapping:
    1 = session started
    2 = page viewed
    3 = product viewed
    4 = added to cart
    5 = began checkout
    6 = entered payment info
    7 = purchased
*/
funnel_stage as (

    select
        *,
        case
            when had_purchase         then 7
            when had_add_payment_info then 6
            when had_begin_checkout   then 5
            when had_add_to_cart      then 4
            when had_view_item        then 3
            when had_page_view        then 2
            when had_session_start    then 1
            else 0
        end                                             as max_funnel_stage,

        case
            when had_purchase         then 'purchase'
            when had_add_payment_info then 'add_payment_info'
            when had_begin_checkout   then 'begin_checkout'
            when had_add_to_cart      then 'add_to_cart'
            when had_view_item        then 'view_item'
            when had_page_view        then 'page_view'
            when had_session_start    then 'session_start'
            else 'unknown'
        end                                             as deepest_stage_name

    from sessions

),

/*
  Channel grouping — normalise the free-text source/medium into
  clean marketing channels for the dashboard dimension filter.
*/
channel_grouped as (

    select
        *,
        case
            when lower(attributed_medium) in ('cpc', 'ppc', 'paid', 'paidsearch')
                then 'Paid Search'
            when lower(attributed_medium) in ('cpm', 'display', 'banner', 'paid_social')
                or lower(attributed_source) in ('facebook', 'instagram', 'twitter', 'linkedin', 'tiktok')
                then 'Paid Social'
            when lower(attributed_medium) = 'email'
                then 'Email'
            when lower(attributed_medium) in ('organic', 'seo')
                or lower(attributed_source) in ('google', 'bing', 'yahoo', 'duckduckgo')
                then 'Organic Search'
            when lower(attributed_medium) in ('referral', 'affiliate')
                then 'Referral'
            when lower(attributed_source) in ('(direct)', 'direct')
                or lower(attributed_medium) in ('(none)', 'none')
                then 'Direct'
            when lower(attributed_medium) in ('social', 'social-network', 'sm')
                then 'Organic Social'
            else 'Other'
        end                                             as channel_group

    from funnel_stage

)

select
    -- Keys
    session_id,
    user_pseudo_id,
    transaction_id,

    -- Time
    session_date,
    session_start_at,
    session_end_at,
    session_duration_seconds,

    -- Attribution
    attributed_source,
    attributed_medium,
    attributed_campaign,
    channel_group,
    device_category,
    country,

    -- Funnel stage reached (numeric for ordering, name for labels)
    max_funnel_stage,
    deepest_stage_name,

    -- Boolean flags — useful for flexible aggregation in BI tools
    had_session_start,
    had_page_view,
    had_view_item,
    had_add_to_cart,
    had_begin_checkout,
    had_add_payment_info,
    had_purchase,

    -- Revenue
    session_revenue,

    -- Engagement
    is_engaged_session,
    total_events,

    -- Convenience: was this session a conversion? (reached purchase)
    had_purchase                                        as is_converted,

    -- Step-to-step progression flags (TRUE = advanced from prior step)
    -- Useful for calculating micro-conversion rates per step
    case when had_page_view        and had_session_start    then true else false end  as progressed_to_page_view,
    case when had_view_item        and had_page_view         then true else false end  as progressed_to_view_item,
    case when had_add_to_cart      and had_view_item         then true else false end  as progressed_to_add_to_cart,
    case when had_begin_checkout   and had_add_to_cart       then true else false end  as progressed_to_checkout,
    case when had_add_payment_info and had_begin_checkout    then true else false end  as progressed_to_payment,
    case when had_purchase         and had_add_payment_info  then true else false end  as progressed_to_purchase,

    -- Metadata
    current_timestamp()                                 as dbt_loaded_at

from channel_grouped