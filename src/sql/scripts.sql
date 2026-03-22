DROP TABLE IF EXISTS green_trips;

CREATE TABLE green_trips (
	lpep_pickup_datetime "timestamp",
	lpep_dropoff_datetime "timestamp",
	pickup_location_id INT,
	dropoff_location_id INT,
	passenger_count INT NULL,
	trip_distance NUMERIC,
	tip_amount NUMERIC,
	total_amount NUMERIC
);


CREATE TABLE processed_events_aggregated (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);