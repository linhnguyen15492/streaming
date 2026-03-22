from producer import get_data_from_url
from kafka import KafkaProducer
from utils.models import ride_serializer, ride_from_row
from tqdm import tqdm
import time

SERVER = 'localhost:9092'
TOPIC = 'rides'

yellow_trip_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
columns = ['PULocationID',
           'DOLocationID',
           'trip_distance',
           'total_amount',
           'tpep_pickup_datetime']
df = get_data_from_url(yellow_trip_url, columns=columns)

producer = KafkaProducer(bootstrap_servers=SERVER, value_serializer=ride_serializer)

for _, row in tqdm(df.iterrows()):
    ride = ride_from_row(row)
    producer.send(topic=TOPIC, value=ride)
    # time.sleep(0.01)

producer.flush()
producer.close()
