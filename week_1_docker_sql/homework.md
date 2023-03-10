## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--iidfile string`

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. 
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 3

note: ```docker run -it --entrypoint=bash python:3.9```
```pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
```
# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20530


Note:
```
select count(*) from green_taxi_trips
where lpep_pickup_datetime > '2019-01-15 0:0:0' 
and lpep_dropoff_datetime <'2019-01-16 0:0:0'
```

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-15


Note:
```
select * from green_taxi_trips  
where trip_distance=(  
	Select max(trip_distance) from green_taxi_trips  
);  
```

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254
- 2: 1282 ; 3: 274


Note:
```
select count(*) as total_trip_per_p from green_taxi_trips
where lpep_pickup_datetime > '2019-01-01 00:00:00' and
	lpep_pickup_datetime < '2019-01-02 00:00:00'
group by passenger_count
```

I got different results:  
| n. of passengers     | total trips|
| ----------- | ----------- |
| 2   |12415       |
| 3     | 1281      |



## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Long Island City/Queens Plaza

Note:
```
SELECT * FROM (
SELECT tip_amount, 
    zpu."Zone" AS pick_up_loc,
	zdo."Zone" AS drop_off_loc
from green_taxi_trips g
left join taxi_zone_lookup zpu
ON g."PULocationID" = zpu."LocationID"
left join taxi_zone_lookup zdo
ON g."DOLocationID" = zdo."LocationID") a
WHERE a."pick_up_loc" = 'Astoria'
ORDER BY tip_amount DESC;
```

## Submitting the solutions

* Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET


## Solution

We will publish the solution here
