![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=googlecloud&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)

# Marketing Funnel Analytics — dbt Project

End-to-end dbt pipeline transforming raw GA4 event data (BigQuery public dataset)
into a clean funnel analytics model with step-by-step conversion rates,
channel attribution, and pre-aggregated metrics for Power BI / Streamlit.

## Business problem

Raw GA4 data is event-level and nested — you can't directly answer
"what % of users who viewed a product actually purchased?" without
substantial transformation. This project builds the transformation layer
that turns raw events into a structured funnel model, enabling:

- Step-to-step drop-off analysis across 7 funnel stages
- Conversion rate by channel, device, campaign, and country
- Revenue attribution to first-touch traffic source
- Daily trend monitoring via the pre-aggregated summary table

## Data architecture

```
GA4 BigQuery public dataset
        │
        ▼
stg_ga4_events     ← flatten nested event_params, add surrogate keys
        │
        ▼
stg_ga4_sessions   ← aggregate to session grain, first-touch attribution
        │
        ├──► fct_funnel_steps    ← one row per session, full funnel flags
        │
        └──► fct_funnel_summary  ← pre-aggregated by date/channel/device
```

## Funnel stages modelled

| Stage | Event name         | Flag column            |
|-------|--------------------|------------------------|
| 1     | session_start      | had_session_start      |
| 2     | page_view          | had_page_view          |
| 3     | view_item          | had_view_item          |
| 4     | add_to_cart        | had_add_to_cart        |
| 5     | begin_checkout     | had_begin_checkout     |
| 6     | add_payment_info   | had_add_payment_info   |
| 7     | purchase           | had_purchase           |

## Quick start

### Prerequisites
- Python 3.8+
- A free Google Cloud account with BigQuery enabled
- dbt Core installed

```bash
pip install dbt-bigquery
```

### Date range

By default, models process Nov 2020 – Jan 2021 (the available public dataset range).
Override with variables:

```bash
dbt run --vars '{"start_date": "20201101", "end_date": "20210131"}'
```

## Key metrics available in fct_funnel_summary

| Metric                        | Column                        |
|-------------------------------|-------------------------------|
| Overall conversion rate       | overall_conversion_rate       |
| Session → page view rate      | rate_session_to_page_view     |
| Page view → product view rate | rate_page_view_to_view_item   |
| Product → cart rate           | rate_view_item_to_add_to_cart |
| Cart → checkout rate          | rate_cart_to_checkout         |
| Checkout → payment rate       | rate_checkout_to_payment      |
| Payment → purchase rate       | rate_payment_to_purchase      |
| Average order value           | avg_order_value               |
| Total revenue                 | total_revenue                 |

## Tests

- `unique` and `not_null` on all primary keys
- `accepted_values` on `max_funnel_stage`, `deepest_stage_name`, `channel_group`
- Custom singular test: `test_funnel_stage_ordering` — verifies logical
  consistency of funnel progression (e.g. no purchase without payment)

## Project structure

```
models/
  staging/
    stg_ga4_events.sql        ← raw event flattening
    stg_ga4_sessions.sql      ← session aggregation + attribution
  marts/
    fct_funnel_steps.sql      ← one row per session (main model)
    fct_funnel_summary.sql    ← pre-aggregated metrics
tests/
  test_funnel_stage_ordering.sql
schema.yml                    ← sources, column docs, and tests
packages.yml                  ← dbt-utils dependency
```
