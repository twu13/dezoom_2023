{{ config(materialized="view") }}

select
    record_id,
    covered_recipient_type,
    nature_of_payment_or_transfer_of_value,
    number_of_payments_included_in_total_amount,
    total_amount_of_payment_usdollars,
    applicable_manufacturer_or_applicable_gpo_making_payment_name,
    applicable_manufacturer_or_applicable_gpo_making_payment_state,
    date_of_payment,
    form_of_payment_or_transfer_of_value,
    related_product_indicator,
    product_category_or_therapeutic_area_1,
    product_category_or_therapeutic_area_2,
    product_category_or_therapeutic_area_3,
    product_category_or_therapeutic_area_4,
    product_category_or_therapeutic_area_5,
    dispute_status_for_publication,
    physician_ownership_indicator,
    indicate_drug_or_biological_or_device_or_medical_supply_1,
    recipient_state,
    covered_recipient_specialty_1
from {{ source("staging", "payments_2020") }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
