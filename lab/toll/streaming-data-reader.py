"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
import psycopg2
import os, sys

load_dotenv()

TOPIC = 'toll_stream'
HOST = os.getenv("PG_HOST")
PORT = os.getenv("PG_PORT")
DATABASE = os.getenv("PG_DATABASE")
USERNAME = os.getenv("PG_USER")
PASSWORD = os.getenv("PG_PASSWORD")

TABLE = os.getenv("PG_TABLE")

print("Connecting to the database")
connection = None
try:
    # connection = mysql.connector.connect(host='mysql', database=DATABASE, user=USERNAME, password=PASSWORD)
    params = {
        "host": HOST,
        "database": DATABASE,
        "user": USERNAME,
        "password": PASSWORD,
        "port": PORT
    }

    connection = psycopg2.connect(**params)

except Exception as e:
    print("Could not connect to database. Please check credentials")
    print("Error: ", e)
    sys.exit(1)
else:
    print("Connected to database")

cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
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
