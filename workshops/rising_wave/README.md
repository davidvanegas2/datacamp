# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:
```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. Yorkville East, Steinway --> CORRECT
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW high_avg_trip_time AS  
 WITH trip_times AS (  
        SELECT  
  CONCAT(tz1.zone, ' -> ', tz2.zone) as trip_pair,  
            AVG(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as avg_trip_time,  
            MIN(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as min_trip_time,  
            MAX(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as max_trip_time  
        FROM trip_data  
        JOIN taxi_zone as tz1  
            ON trip_data.pulocationid = tz1.location_id  
  JOIN taxi_zone as tz2  
            ON trip_data.dolocationid = tz2.location_id  
  GROUP BY trip_pair  
    )  
    SELECT  
  trip_pair,  
        avg_trip_time  
    FROM trip_times  
    WHERE avg_trip_time = (SELECT MAX(avg_trip_time) FROM trip_times);

--          trip_pair          | avg_trip_time 
-- ----------------------------+---------------
--  Yorkville East -> Steinway | 23:59:33
-- (1 row)

```

</details>

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. 1 --> CORRECT

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW high_avg_trip_time AS  
 WITH trip_times AS (  
        SELECT  
  CONCAT(tz1.zone, ' -> ', tz2.zone) as trip_pair,  
            AVG(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as avg_trip_time,  
            MIN(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as min_trip_time,  
            MAX(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as max_trip_time,  
            COUNT(*) as num_trips  
        FROM trip_data  
        JOIN taxi_zone as tz1  
            ON trip_data.pulocationid = tz1.location_id  
  JOIN taxi_zone as tz2  
            ON trip_data.dolocationid = tz2.location_id  
  GROUP BY trip_pair  
    )  
    SELECT  
  trip_pair,  
        avg_trip_time,  
        num_trips  
    FROM trip_times  
    WHERE avg_trip_time = (SELECT MAX(avg_trip_time) FROM trip_times);

--          trip_pair          | avg_trip_time | num_trips 
-- ----------------------------+---------------+-----------
--  Yorkville East -> Steinway | 23:59:33      |         1
-- (1 row)
```

</details>

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW top_3_busiest_zones AS  
 SELECT  zone,  
        COUNT(*) as num_pickups  
    FROM trip_data  
    JOIN taxi_zone  
        ON trip_data.pulocationid = taxi_zone.location_id  
  WHERE tpep_pickup_datetime >= (SELECT MAX(tpep_pickup_datetime) - INTERVAL '17 hours' FROM trip_data)  
    GROUP BY zone  
  ORDER BY num_pickups DESC  
 LIMIT 3;ip_time = (SELECT MAX(avg_trip_time) FROM trip_times);

--         zone         | num_pickups 
-- ---------------------+-------------
--  LaGuardia Airport   |          19
--  Lincoln Square East |          17
--  JFK Airport         |          17
-- (3 rows)

```

</details>

> Written with [StackEdit](https://stackedit.io/).
