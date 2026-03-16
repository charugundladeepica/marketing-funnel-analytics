/*
  test_funnel_stage_ordering.sql

  Singular test: verifies that the stage flags are logically consistent.
  A session cannot reach checkout without having added to cart first, etc.

  Any rows returned = test failure.
*/

with

funnel as (

    select * from `project-1208edf9-ccf5-4444-8b4`.`marketing_funnel_dev_marts`.`fct_funnel_steps`

)

select
    session_id,
    'checkout_without_cart' as violation_type
from funnel
where had_begin_checkout = true
  and had_add_to_cart    = false

union all

select
    session_id,
    'payment_without_checkout'
from funnel
where had_add_payment_info = true
  and had_begin_checkout   = false

union all

select
    session_id,
    'purchase_without_payment'
from funnel
where had_purchase         = true
  and had_add_payment_info = false

union all

select
    session_id,
    'negative_revenue_on_purchase'
from funnel
where had_purchase    = true
  and session_revenue < 0