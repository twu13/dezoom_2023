{{ config(materialized='view') }}

select * from {{ source('staging', 'payments_2021') }}
limit 100