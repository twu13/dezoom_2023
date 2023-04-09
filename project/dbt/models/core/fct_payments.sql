with
    data_2019 as (select *, '2019' as data_version from {{ ref("stg_2019_payments") }}),
    data_2020 as (select *, '2020' as data_version from {{ ref("stg_2020_payments") }}),
    data_2021 as (select *, '2021' as data_version from {{ ref("stg_2021_payments") }}),
    payments_unioned as (
        select *
        from data_2019
        union all
        select *
        from data_2020
        union all
        select *
        from data_2021
    )
select *
from payments_unioned

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
