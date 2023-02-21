## Question 1
Code for creating the full `fact_trips` table:  
```
dbt run --select fact_trips --var 'is_test_run: false'
```
Query for counting the table:  
```
SELECT COUNT(*)
FROM `dtc-de-course-375205.dezoomcamp.fact_trips`
WHERE EXTRACT(
        YEAR
        FROM pickup_datetime
    ) = 2019
    OR EXTRACT(
        YEAR
        FROM pickup_datetime
    ) = 2020;
```
There are `61,539,764` rows

## Question 2
Code for calculating the ratio:  
```
SELECT SUM(
        CASE
            WHEN service_type = 'Yellow' THEN 1
            ELSE 0
        END
    ) / SUM(
        CASE
            WHEN service_type = 'Green' THEN 1
            ELSE 0
        END
    ) AS ratio
FROM `dtc-de-course-375205.dezoomcamp.fact_trips`
WHERE EXTRACT(
        YEAR
        FROM pickup_datetime
    ) = 2019
    OR EXTRACT(
        YEAR
        FROM pickup_datetime
    ) = 2020;
```
The distribution is `89.9 / 10.1`

## Question 3
Code for creating `stg_fhv_tripdata`:  
```
{{ config(materialized='view') }}
 
select
    cast(dispatching_base_num as string) as dispatching_base_num,		
    cast(pickup_datetime as timestamp) as pickup_datetime,	
    cast(dropOff_datetime as timestamp) as dropOff_datetime,		
    cast(PUlocationID as integer) as PUlocationID,			
    cast(DOlocationID as integer) as DOlocationID,			
    cast(SR_Flag as string) as SR_Flag,
    cast(Affiliated_base_number as string) as Affiliated_base_number
from {{ source('staging','fhv_data_internal_nonp') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
```
SELECT COUNT(*)
FROM `dtc-de-course-375205.dezoomcamp.stg_fhv_tripdata`
WHERE EXTRACT(
        YEAR
        FROM pickup_datetime
    ) = 2019;
```
There are `43,244,696` rows

## Question 4
Code for creating `fact_trips_fhv.sql`:  
```
{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    dispatching_base_num,		
    pickup_datetime,	
    dropOff_datetime,		
    PUlocationID,			
    DOlocationID,			
    SR_Flag,
    Affiliated_base_number,
    pickup_zone.locationid as pickup_locationid,
    pickup_zone.borough as pickup_borough,
    dropoff_zone.locationid as dropoff_locationid,
    dropoff_zone.borough as dropoff_borough
from {{ ref('stg_fhv_tripdata') }}
inner join dim_zones as pickup_zone
on PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on DOlocationID = dropoff_zone.locationid
```
```
SELECT COUNT(*)
FROM `dtc-de-course-375205.dezoomcamp.fact_trips_fhv`
WHERE EXTRACT(
        YEAR
        FROM pickup_datetime
    ) = 2019;
```
There are `22,998,722` rows

## Question 5
```
SELECT COUNT(*) AS rides,
    EXTRACT(
        MONTH
        FROM pickup_datetime
    ) AS month
FROM `dtc-de-course-375205.dezoomcamp.fact_trips_fhv`
WHERE EXTRACT(
        YEAR
        FROM pickup_datetime
    ) = 2019
GROUP BY 2
ORDER BY rides DESC;
```
`January` is the month with the most rides in 2019