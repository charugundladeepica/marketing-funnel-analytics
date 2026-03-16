{{
    config(
        materialized='view',
        description='Cleaned and typed GA4 event rows from the BigQuery public dataset. One row per event.'
    )
}}

/*
  Source: bigquery-public-data.ga4_obfuscated_sample_ecommerce
  Docs:   https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset

  This staging model:
    - Flattens nested event_params into named columns
    - Casts all fields to correct types
    - Filters out bot/internal traffic
    - Creates a stable surrogate key
*/

with

raw_events as (

    select
        -- Session & user identifiers
        cast(event_date as date format 'YYYYMMDD')  as event_date,
        timestamp_micros(event_timestamp)            as event_timestamp,
        event_name,
        user_pseudo_id,

        -- Extract session_id from event_params (nested array)
        (
            select value.int_value
            from unnest(event_params)
            where key = 'ga_session_id'
        )                                            as ga_session_id,

        -- Extract engagement signal
        (
            select value.string_value
            from unnest(event_params)
            where key = 'session_engaged'
        )                                            as session_engaged,

        -- Page context
        (
            select value.string_value
            from unnest(event_params)
            where key = 'page_title'
        )                                            as page_title,

        (
            select value.string_value
            from unnest(event_params)
            where key = 'page_location'
        )                                            as page_location,

        -- Traffic source
        traffic_source.medium                        as traffic_medium,
        traffic_source.source                        as traffic_source,
        traffic_source.name                          as campaign_name,

        -- Device
        device.category                              as device_category,
        device.operating_system                      as operating_system,
        geo.country                                  as country,

        -- Ecommerce (populated for purchase events)
        ecommerce.transaction_id,
        ecommerce.purchase_revenue,
        ecommerce.tax_value,
        ecommerce.shipping_value,

        -- Item count for add_to_cart / purchase events
        array_length(items)                          as item_count

    from {{ source('ga4_public', 'events_*') }}

    where
        -- Use date range macro so this is easy to adjust
        _table_suffix between
            cast({{ var('start_date', "'20201101'") }} as string)
            and cast({{ var('end_date', "'20210131'") }} as string)
            
        -- Filter out known bot/test traffic
        and traffic_source.medium != '(not set)'
        and user_pseudo_id is not null

),

with_keys as (

    select
        -- Surrogate key: user + session + timestamp (unique per event)
        {{ dbt_utils.generate_surrogate_key([
            'user_pseudo_id',
            'ga_session_id',
            'event_timestamp'
        ]) }}                                        as event_id,

        -- Session-level key (used to join across funnel steps)
        {{ dbt_utils.generate_surrogate_key([
            'user_pseudo_id',
            'ga_session_id'
        ]) }}                                        as session_id,

        *

    from raw_events

)

select * from with_keys
