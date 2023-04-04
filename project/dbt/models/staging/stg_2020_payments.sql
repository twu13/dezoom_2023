{{ config(materialized='view') }}

select * from {{ source('staging', 'payments_2020') }}
limit 100