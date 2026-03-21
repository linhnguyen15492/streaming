import pandas as pd
from kafka import KafkaProducer
import json

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"

columns = [
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
    "tpep_pickup_datetime",
]

df = pd.read_parquet(URL, columns=columns).head(1000)
print(df.head())

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer.send("bank_branch", {'atm_id': 1, 'trans_id': 100})
producer.send("bank_branch", {'atm_id': 2, 'trans_id': 101})
producer.flush()
producer.close()
