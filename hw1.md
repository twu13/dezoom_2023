## Question 1
`docker build --help`  
`--iidfile string`  

## Question 2
`docker run -it --entrypoint=bash python:3.9`  
`pip list`  
There are 3 packages:
1. pip
2. setuptools
3. wheel

## Question 3
```
SELECT COUNT(*)
FROM public.green_taxi_data
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-15'
    AND CAST(lpep_dropoff_datetime AS DATE) = '2019-01-15';
```
There were `20530` taxi trips totally made on January 15

## Question 4
```
SELECT CAST(lpep_pickup_datetime AS DATE)
FROM public.green_taxi_data
WHERE trip_distance = (
        SELECT max(trip_distance)
        from public.green_taxi_data
    );
```
`2019-01-15` was the pickup day with the largest trip distance

## Question 5
```
SELECT COUNT(*) AS rides,
    passenger_count
FROM public.green_taxi_data
WHERE (
        CAST(lpep_pickup_datetime AS DATE) = '2019-01-01'
        OR CAST(lpep_dropoff_datetime AS DATE) = '2019-01-01'
    )
    AND passenger_count IN (2, 3)
GROUP BY passenger_count;
```
`1282` rides had 2 passengers and `254` rides had 3 passengers

## Question 6
```
SELECT tip_amount,
    zpu."Zone" AS "pickup_loc",
    zdo."Zone" AS "dropoff_loc"
FROM green_taxi_data t,
    green_taxi_zone zpu,
    green_taxi_zone zdo
WHERE t."PULocationID" = zpu."LocationID"
    AND t."DOLocationID" = zdo."LocationID"
    AND zpu."Zone" = 'Astoria'
ORDER BY tip_amount DESC
LIMIT 1;
```
`"Long Island City/Queens Plaza"` was the dropoff zone that had the largest tip
