"""
Python script to connect to Kafka and consume messages using PySpark
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Kafka Spark Structured Streaming") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

green_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

green_stream_df = green_stream_df \
    .select(F.from_json(F.col("value").cast("STRING"), schema).alias("data")) \
    .select("data.*")


def peek(mini_batch_df, batch_id):
    """
    Function to peek at the data
    :param mini_batch_df:
    :param batch_id:
    :return:
    """
    print(f"Processing batch: {batch_id}")
    first_row = mini_batch_df.take(1)

    if first_row:
        print(first_row[0])


# query = green_stream_df \
#     .writeStream \
#     .foreachBatch(peek) \
#     .start()
#
# query.awaitTermination()


# Question 7: Most popular destination
# Now let's finally do some streaming analytics.
# We will see what's the most popular destination currently based on our stream of data
# (which ideally we should have sent with delays)
#
# This is how you can do it:
# - Add a column "timestamp" using the current_timestamp function
# - Group by:
#   - 5 minutes window based on the timestamp column (F.window(col("timestamp"), "5 minutes"))
#   - "DOLocationID"
# - Order by count

green_stream_df = green_stream_df \
    .withColumn("timestamp", F.current_timestamp())

popular_destinations = green_stream_df \
    .groupBy(
        F.window(F.col("timestamp"), "5 minutes"),
        F.col("DOLocationID")
    ) \
    .count() \
    .orderBy(F.col("count").desc())

query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

