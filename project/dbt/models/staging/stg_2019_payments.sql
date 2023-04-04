{{ config(materialized='view') }}

select * from {{ source('staging', 'payments_2019') }}
limit 100