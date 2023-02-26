## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHVHV 2021-06 data found here. [FHVHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz )


### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- 3.3.2   * 
- 2.1.4
- 1.2.3
- 5.4
</br></br>

Note:
pyspark.__file__  
output: '/opt/homebrew/Cellar/apache-spark/3.3.2/libexec/python/pyspark/__init__.py'


### Question 2: 

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- 2MB
- 24MB   *
- 100MB
- 250MB
</br></br>

Note: 
```python
df = df.repartition(12)
df.write.parquet('fhvhv/2021/06/')
!ls -lh fhvhv/2021/06
```

### Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 308,164
- 12,856
- 452,470   *
- 50,982
</br></br>


Code:
```python
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')\
    .filter(df['pickup_datetime'].cast('date')=="2021-06-15") \
    .count()
```

### Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours   *
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours
</br></br>


Code:  
```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('trip_duration', F.round((F.column('dropoff_datetime').cast("long") - F.column('pickup_datetime').cast("long"))/3600,2)) \
    .select('trip_duration','pickup_datetime', 'dropoff_datetime') \
    .sort('trip_duration', ascending=False) \
    .show(5)
```


### Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 80
- 443
- 4040   *
- 8080
</br></br>


### Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- East Chelsea
- Astoria
- Union Sq
- Crown Heights North  *
</br></br>


Code:
```python
df_result = spark.sql("""
SELECT
    Zone,
    count(1) as number_rides
FROM
    fhvhv_taxi f
JOIN taxi_zone t
ON f.PULocationID = t.LocationID
GROUP BY 
    Zone
ORDER BY 
    number_rides DESC
""")
df_result.show()
```
+--------------------+------------+
|                Zone|number_rides|
+--------------------+------------+
| Crown Heights North|      231279|


## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here
