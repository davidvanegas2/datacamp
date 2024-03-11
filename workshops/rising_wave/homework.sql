SELECT * FROM trip_data;
SELECT * FROM taxi_zone;

-- QUESTION 0
-- What are the dropoff taxi zones at the latest dropoff times?
CREATE MATERIALIZED VIEW latest_dropoff AS
SELECT
  tpep_dropoff_datetime,
  zone
FROM trip_data
JOIN taxi_zone
ON trip_data.dolocationid = taxi_zone.location_id
WHERE tpep_dropoff_datetime = (SELECT MAX(tpep_dropoff_datetime) FROM trip_data);

-- QUESTION 1
-- Create a materialized view to compute the average, min and max trip time between each taxi zone.

-- Note that we consider the do not consider a->b and b->a as the same trip pair. So as an example, you would consider the following trip pairs as different pairs:
-- 1. Yorkville East -> Steinway
-- 2. Steinway -> Yorkville East

-- From this MV, find the pair of taxi zones with the highest average trip time.

DROP MATERIALIZED VIEW IF EXISTS high_avg_trip_time;
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

-- BONUS QUESTION
-- Create an MV which can identify anomalies in the data.
-- For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.

DROP MATERIALIZED VIEW IF EXISTS trip_time_anomalies;
CREATE MATERIALIZED VIEW trip_time_anomalies AS
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
        avg_trip_time,
        min_trip_time,
        max_trip_time,
        CASE
            WHEN min_trip_time * 10 < avg_trip_time THEN 'Anomaly MIN'
            WHEN avg_trip_time * 10 < max_trip_time THEN 'Anomaly MAX'
            ELSE 'Normal'
        END as anomaly,
        min_trip_time * 10 as min_trip_time_10x,
        avg_trip_time * 10 as max_trip_time_10x
    FROM trip_times;



-- QUESTION 2
-- Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

DROP MATERIALIZED VIEW IF EXISTS high_avg_trip_time;
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

-- QUESTION 3
-- From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?

-- For example if the latest pickup time is 2020-01-01 17:00:00, then the query should return the top 3 busiest zones
-- from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

DROP MATERIALIZED VIEW IF EXISTS top_3_busiest_zones;
CREATE MATERIALIZED VIEW top_3_busiest_zones AS
    SELECT
        zone,
        COUNT(*) as num_pickups
    FROM trip_data
    JOIN taxi_zone
        ON trip_data.pulocationid = taxi_zone.location_id
    WHERE tpep_pickup_datetime >= (SELECT MAX(tpep_pickup_datetime) - INTERVAL '17 hours' FROM trip_data)
    GROUP BY zone
    ORDER BY num_pickups DESC
    LIMIT 3;



