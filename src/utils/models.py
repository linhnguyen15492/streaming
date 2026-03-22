from dataclasses import dataclass
import json

from pyarrow import json_


@dataclass
class Ride:
    id: int
    name: str


class GreenTrip:
    def __init__(self,
                 lpep_pickup_datetime: float,
                 lpep_dropoff_datetime: float,
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
