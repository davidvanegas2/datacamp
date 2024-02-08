-- Create an external table to query the data in BigQuery
CREATE OR REPLACE EXTERNAL TABLE `de-course-411516.terraform_dataset.external_green_tripdata`
OPTIONS (
  format = "PARQUET",
    uris = ["gs://terraform_bucket_dfvanegas/data/data/green_tripdata_2022-*.parquet"]
);

-- Create table in BigQuery
CREATE OR REPLACE TABLE de-course-411516.terraform_dataset.green_tripdata_non_partitoned AS
SELECT * FROM de-course-411516.terraform_dataset.external_green_tripdata;

-- What is count of records for the 2022 Green Taxi Data?
SELECT COUNT(*) FROM de-course-411516.terraform_dataset.green_tripdata_non_partitoned;

-- What is the count of the distinct number of PULocationIDs for the entire dataset
SELECT COUNT(DISTINCT PULocationID) FROM de-course-411516.terraform_dataset.green_tripdata_non_partitoned;
SELECT COUNT(DISTINCT PULocationID) FROM de-course-411516.terraform_dataset.external_green_tripdata;

-- How many records have a fare_amount of 0?
SELECT COUNT(*) FROM de-course-411516.terraform_dataset.green_tripdata_non_partitoned WHERE fare_amount = 0;

-- Creating a partitioned table by lpep_pickup_datetime and clustering by PUlocationID
CREATE OR REPLACE TABLE de-course-411516.terraform_dataset.green_tripdata_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS
SELECT * FROM de-course-411516.terraform_dataset.green_tripdata_non_partitoned;

-- Retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
SELECT DISTINCT PULocationID
FROM de-course-411516.terraform_dataset.green_tripdata_non_partitoned
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

-- Retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive) from the partitioned table
SELECT DISTINCT PULocationID
FROM de-course-411516.terraform_dataset.green_tripdata_partitioned
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

-- Count how many records are in the non partitioned table
SELECT COUNT(*) FROM de-course-411516.terraform_dataset.green_tripdata_non_partitoned;
