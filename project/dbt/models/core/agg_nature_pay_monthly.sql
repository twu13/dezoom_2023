{{ config(materialized="table") }}

with pay_data as (select * from {{ ref("fct_payments") }})
select
    nature_of_payment_or_transfer_of_value as pay_nat,
    extract(year from date_of_payment) as pay_year,
    extract(month from date_of_payment) as pay_month,
    recipient_state,
    sum(cast(total_amount_of_payment_usdollars as numeric)) as total_pay_usd,
    sum(cast(number_of_payments_included_in_total_amount as numeric)) as total_payments
from pay_data
group by 1, 2, 3, 4
