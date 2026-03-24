from dataclasses import dataclass
import json
import dataclasses


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)


def ride_serializer(ride):
    return json.dumps(dataclasses.asdict(ride)).encode('utf-8')


class GreenTrip:
    def __init__(self,
                 lpep_pickup_datetime,
                 lpep_dropoff_datetime,
                 PULocationID: int,
                 DOLocationID: int,
                 passenger_count: int,
                 trip_distance: float,
                 tip_amount: float,
                 total_amount: float):
        self.lpep_pickup_datetime = lpep_pickup_datetime
        self.lpep_dropoff_datetime = lpep_dropoff_datetime
        self.PULocationID = PULocationID
        self.DOLocationID = DOLocationID
        self.passenger_count = passenger_count
        self.trip_distance = trip_distance
        self.tip_amount = tip_amount
        self.total_amount = total_amount

    @staticmethod
    def serialize(data: "GreenTrip"):
        json_str = json.dumps(data.__dict__)
        return json_str.encode("utf-8")

    @staticmethod
    def deserialize(data: "GreenTrip"):
        json_str = data.decode("utf-8")
        dict_data = json.loads(json_str)
        return GreenTrip(**dict_data)


class Rectangle:
    def __init__(self, width, height):
        self.width = width
        self.height = height

    def serialize(self):
        json_str = json.dumps(self.__dict__)
        return json_str.encode("utf-8")
