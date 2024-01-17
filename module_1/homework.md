# Module 1 - Homework

## 1st part - Docker and SQL
### Question #1 - Knowing docker tags
Which tag has the following text? -  _Automatically remove the container when it exits_

 1. `--delete`
 2. `--rc`
 3. `--rmc`
 4. `--rm`

The answer is the 4th option: `--rm`

### Question #2 - Understanding docker first run
Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use  `pip list`  ).

What is version of the package  _wheel_  ?

 1. 0.42.0
 2. 1.0.0
 3. 23.0.1
 4. 58.1.0

The answer is the 1st option: `0.42.0`

### Prepare Postgres

Run Postgres and load data as shown in the videos We'll use the green taxi trips from September 2019:

`wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz`

You will also need the dataset with zones:

`wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv`

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

### Question #3 - Count records

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18.

Remember that  `lpep_pickup_datetime`  and  `lpep_dropoff_datetime`  columns are in the format timestamp (date and hour+min+sec) and not in date.

 1. 15767
 2. 15612
 3. 15859
 4. 89009

The answer is the 2nd option: `15612`
***SQL Query:***

    SELECT COUNT(1)
    FROM public.yellow_taxi_trips
    WHERE
    	CAST("lpep_pickup_datetime" AS DATE) = '2019-09-18'
    	AND CAST("lpep_dropoff_datetime" AS DATE) = '2019-09-18';

### Question #4 - Largest trip for each day

Which was the pick up day with the largest trip distance Use the pick up time for your calculations.

 1. 2019-09-18
 2. 2019-09-16
 3. 2019-09-26
 4. 2019-09-21

The correct answer is 3rd option: `2019-09-26`
***SQL Query:***

    SELECT 
    	CAST("lpep_pickup_datetime" AS DATE) AS "day",
    	MAX("lpep_dropoff_datetime" - "lpep_pickup_datetime") AS "trip_duration"
    FROM public.yellow_taxi_trips
    GROUP BY CAST("lpep_pickup_datetime" AS DATE)
    ORDER BY "trip_duration" DESC
    LIMIT 1;

### Question #5 - The number of passengers

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

 1. "Brooklyn" "Manhattan" "Queens"
 2. "Bronx" "Brooklyn" "Manhattan"
 3. "Bronx" "Manhattan" "Queens"
 4. "Brooklyn" "Queens" "Staten Island"

The correct answer is the 1st option: `"Brooklyn" "Manhattan" "Queens"`
***SQL Query***

    SELECT
    	z."Borough" as "Start_Borough",
    	SUM(t."total_amount") as "sum_total"
    FROM public.yellow_taxi_trips t
    JOIN public.taxi_zones z
    	ON t."PULocationID" = z."LocationID"
    JOIN public.taxi_zones p
    	ON t."DOLocationID" = p."LocationID"
    WHERE CAST("lpep_pickup_datetime" AS DATE) = '2019-09-18'
    GROUP BY z."Borough"
    ORDER BY "sum_total" DESC;

### Question #6 - Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's  `tip`  , not  `trip`

 1. Central Park
 2. Jamaica
 3. JFK Airport
 4. Long Island City/Queens Plaza

The correct answer is 3rd option: `JFK Airport`
***SQL Query***

    SELECT
    	CAST("lpep_pickup_datetime" AS DATE) AS "day",
    	z."Zone" as "Start_Zone",
    	p."Zone" as "Finish_Zone",
    	t."tip_amount"
    FROM public.yellow_taxi_trips t
    JOIN public.taxi_zones z
    	ON t."PULocationID" = z."LocationID"
    JOIN public.taxi_zones p
    	ON t."DOLocationID" = p."LocationID"
    WHERE z."Zone" = 'Astoria'
    ORDER BY t."tip_amount" DESC
    LIMIT 1;

> Written with [StackEdit](https://stackedit.io/).
