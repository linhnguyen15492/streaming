"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os, sys
from src.utils import postgres_db

load_dotenv()

HOST = os.getenv("PG_HOST")
PORT = os.getenv("PG_PORT")
DATABASE = os.getenv("PG_DATABASE")
USERNAME = os.getenv("PG_USER")
PASSWORD = os.getenv("PG_PASSWORD")

TABLE = os.getenv("PG_TABLE")

SERVER = 'redpanda:9092'
TOPIC = 'toll_stream'

print("Connecting to the database")
params = {
    "host": HOST,
    "database": DATABASE,
    "user": USERNAME,
    "password": PASSWORD,
    "port": PORT
}

connection = postgres_db.connect_to_database(**params)
if connection is None:
    sys.exit("Unable to connect to the database")

cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC, bootstrap_servers=[SERVER])
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")

for msg in consumer:
    # Extract information from kafka
    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehicle_id, vehicle_type, plaza_id) = message.split(",")

    date_obj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table
    sql = f"insert into {TABLE} values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehicle_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()
