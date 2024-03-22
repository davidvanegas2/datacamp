"""
Python script to connect to Kafka server
"""

import json
import time

from kafka import KafkaProducer
import pandas as pd

SERVER = 'localhost:9092'


def json_serializer(data):
    """
    Function to serialize data to JSON
    :param data:
    :return:
    """
    return json.dumps(data).encode('utf-8')


def connect_kafka():
    """
    Main function to connect to Kafka server
    :return:
    """
    kfk_producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=json_serializer
    )

    print(kfk_producer.bootstrap_connected())

    return kfk_producer


def send_data(kafka_producer, topic, data):
    """
    Function to send data to Kafka server
    :param kafka_producer:
    :param topic:
    :param data:
    :return:
    """
    kafka_producer.send(topic, data)


def flush_data(kafka_producer):
    """
    Function to flush data to Kafka server
    :param kafka_producer:
    :return:
    """
    kafka_producer.flush()


def calculate_time(t0, t1, operation):
    """
    Function to calculate time
    :param t0:
    :param t1:
    :param operation:
    :return:
    """
    print(f"Time taken to {operation}: {t1 - t0}")


def send_dummy_data(kafka_producer):
    """
    Function to send dummy data to Kafka server
    :param kafka_producer:
    :return:
    """
    topic_name = 'test-topic'
    t0_0 = time.time()
    for i in range(10):
        data = {'number': i}
        send_data(kafka_producer, topic_name, data)
        print(f"Data sent: {data}")
        time.sleep(0.05)
    t0_1 = time.time()
    flush_data(kafka_producer)
    t1_1 = time.time()
    calculate_time(t0_0, t0_1, "send data")
    calculate_time(t0_1, t1_1, "flush data")


def read_csv(file_path):
    """
    Function to read CSV file
    :param file_path:
    :return:
    """
    return pd.read_csv(file_path, low_memory=False)


def select_columns(df: pd.DataFrame, columns: list):
    """
    Function to select columns from DataFrame
    :param df:
    :param columns:
    :return:
    """
    return df[columns]


def sending_taxi_data(kafka_producer):
    """
    Function to send taxi data to Kafka server
    :param kafka_producer:
    :return:
    """
    topic_name = 'green-trips'
    df = read_csv('green_tripdata_2019-10.csv')
    columns = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]
    df = select_columns(df, columns)

    t0 = time.time()
    for row in df.itertuples(index=False):
        row_dict = {col: getattr(row, col) for col in row._fields}
        send_data(kafka_producer, topic_name, row_dict)

    flush_data(kafka_producer)
    t1 = time.time()
    calculate_time(t0, t1, "send taxi data")


if __name__ == '__main__':
    producer = connect_kafka()
    # send_dummy_data(producer)
    sending_taxi_data(producer)
