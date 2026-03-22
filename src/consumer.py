from kafka import KafkaConsumer
from utils.models import GreenTrip
from utils import postgres_db
from dotenv import load_dotenv
import os
import sys
from datetime import datetime

load_dotenv()

HOST = os.getenv("PG_HOST")
PORT = os.getenv("PG_PORT")
DATABASE = os.getenv("PG_DATABASE")
USERNAME = os.getenv("PG_USER")
PASSWORD = os.getenv("PG_PASSWORD")
TABLE = os.getenv("PG_TABLE")


def stream_consumer(consumer):
    connection = postgres_db.connect_to_database(HOST, PORT, DATABASE, USERNAME, PASSWORD)
    if connection is None:
        print("Connection to Postgres failed")
        sys.exit(1)

    connection.autocommit = True
    cursor = connection.cursor()

    count = 0
    for data in consumer:
        trip = data.value
        # pickup_datetime = datetime.fromtimestamp(trip.lpep_pickup_datetime / 1000)
        # dropoff_datetime = datetime.fromtimestamp(trip.lpep_dropoff_datetime / 1000)

        pickup_datetime = datetime.strptime(trip.lpep_pickup_datetime, '%Y-%m-%d %H:%M:%S')
        dropoff_datetime = datetime.strptime(trip.lpep_dropoff_datetime, '%Y-%m-%d %H:%M:%S')

        cursor.execute(
            """
            INSERT INTO green_trips (lpep_pickup_datetime, 
                                lpep_dropoff_datetime, 
                                pickup_location_id, 
                                dropoff_location_id, 
                                passenger_count, 
                                trip_distance,
                                tip_amount,
                                total_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (pickup_datetime,
             dropoff_datetime,
             trip.PULocationID,
             trip.DOLocationID,
             trip.passenger_count,
             trip.trip_distance,
             trip.tip_amount,
             trip.total_amount)
        )
        count += 1
        print(f"consume {count} rows")


def main():
    server = 'localhost:9092'
    topic = "green-trips"

    consumer = KafkaConsumer(topic,
                             group_id=None,
                             bootstrap_servers=[server],
                             auto_offset_reset='earliest',
                             value_deserializer=GreenTrip.deserialize)

    stream_consumer(consumer)


if __name__ == "__main__":
    main()
