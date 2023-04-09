{{ config(materialized="table") }}

with pay_data as (select * from {{ ref("fct_payments") }})
select
    applicable_manufacturer_or_applicable_gpo_making_payment_name as mfc_gpo,
    extract(year from date_of_payment) as pay_year,
    extract(month from date_of_payment) as pay_month,
    sum(cast(total_amount_of_payment_usdollars as numeric)) as total_pay_usd,
    sum(cast(number_of_payments_included_in_total_amount as numeric)) as total_payments,
    count(
        case when nature_of_payment_or_transfer_of_value = 'Entertainment' then 1 end
    ) as pay_type_entertain,
    count(
        case when nature_of_payment_or_transfer_of_value = 'Gift' then 1 end
    ) as pay_type_gift,
    count(
        case when nature_of_payment_or_transfer_of_value = 'Consulting Fee' then 1 end
    ) as pay_type_consult,
    count(
        case
            when
                nature_of_payment_or_transfer_of_value
                = 'Compensation for services other than consulting, including serving as faculty or as a speaker at a venue other than a continuing education program'
            then 1
        end
    ) as pay_type_other
from pay_data
group by 1, 2, 3
