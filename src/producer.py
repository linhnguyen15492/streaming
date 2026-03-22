import pandas as pd
from kafka import KafkaProducer
import time
from utils.models import GreenTrip
from tqdm import tqdm


def get_data_from_url(url, columns):
    df = pd.read_parquet(url, columns=columns)
    df.fillna(0, inplace=True)
    # print(df.dtypes)
    return df


def transform(row: pd.Series) -> pd.Series:
    row['lpep_pickup_datetime'] = int(row['lpep_pickup_datetime'].timestamp() * 1000)
    row['lpep_dropoff_datetime'] = int(row['lpep_dropoff_datetime'].timestamp() * 1000)

    return row


def transform_ts_str(row: pd.Series) -> pd.Series:
    row['lpep_pickup_datetime'] = str(row['lpep_pickup_datetime'])
    row['lpep_dropoff_datetime'] = str(row['lpep_dropoff_datetime'])

    return row


def get_green_trip_from_row(row: pd.Series) -> GreenTrip:
    return GreenTrip(row['lpep_pickup_datetime'],
                     row['lpep_dropoff_datetime'],
                     row['PULocationID'],
                     row['DOLocationID'],
                     row['passenger_count'],
                     row['trip_distance'],
                     row['tip_amount'],
                     row['total_amount'])


def stream_data(producer: KafkaProducer, topic: str, df: pd.DataFrame) -> bool:
    t0 = time.time()
    count = 0
    try:
        for _, row in tqdm(df.iterrows()):
            # row = transform(row)
            row = transform_ts_str(row)
            trip = get_green_trip_from_row(row)
            producer.send(topic=topic, value=trip)
            count += 1
            # time.sleep(0.01)

        producer.flush()
        t1 = time.time()
        print(f'took {(t1 - t0):.2f} seconds to send {count} records')
        producer.close()

        return True
    except Exception as e:
        print("Error while streaming data:", e)

    return False


def main():
    kafka_server = "localhost:9092"
    topic = "green-trips"

    green_trip_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

    columns = [
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "tip_amount",
        "total_amount",
    ]

    producer = KafkaProducer(
        bootstrap_servers=[kafka_server],
        value_serializer=GreenTrip.serialize)

    df = get_data_from_url(green_trip_url, columns)
    result = stream_data(producer, topic, df)
    if result:
        print("streaming data successfully completed")


if __name__ == "__main__":
    main()
