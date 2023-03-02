## Question 1
```
import pyspark
pyspark.__version__
```
The version is `3.2.1`

## Question 2
```
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-06.csv')

df = df.repartition(12)

df.write.parquet('fhvhv/2021/06/')
```
Average size of the files is `24MB`

## Question 3
```
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df_fhvhv = spark.read.parquet('fhvhv/2021/06/')
df_fhvhv.registerTempTable('fhvhv')

spark.sql("""
SELECT count(1)
FROM fhvhv
WHERE EXTRACT(
        MONTH
        FROM pickup_datetime
    ) = 6
    AND EXTRACT(
        DAY
        FROM pickup_datetime
    ) = 15
""").show()
```
There were `452,470` taxi trips on June 15

## Question 4
Using `df_fhvhv` from code for Question 3:  
```
# Calculate Time difference in Hours
hour_dur=df_fhvhv.withColumn('pickup_datetime',F.to_timestamp(F.col('pickup_datetime')))\
  .withColumn('dropoff_datetime',F.to_timestamp(F.col('dropoff_datetime')))\
  .withColumn('DiffInSeconds',F.col("dropoff_datetime").cast("long") - F.col('pickup_datetime').cast("long"))\
  .withColumn("DiffInHours",F.round(F.col("DiffInSeconds")/3600))
hour_dur.sort(F.col("DiffInHours").desc()).show()
```
The longest trip was `66.87 Hours`

## Question 5
Spark's UI runs on port `4040` by default

## Question 6
```
spark.sql("""
SELECT PULocationID,
    COUNT(*) as total
FROM fhvhv
GROUP BY PULocationID
ORDER BY total DESC """).show()
```
Location ID `61`, `Crown Heights North`, is the most frequent pickup zone