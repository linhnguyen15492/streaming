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

CREATE TABLE pulse_sessions (
	window_start TIMESTAMP(3),
	window_end TIMESTAMP(3),
	PULocationID INT,
	num_trips BIGINT,
	total_revenue "numeric",
	PRIMARY KEY (window_start, window_end, PULocationID)
);


SELECT
	PULocationID,
	num_trips,
	window_start,
	window_end,
	(window_end - window_start) AS session_duration
FROM
	pulse_sessions
ORDER BY
	num_trips DESC;


CREATE TABLE tip_aggregate (
	window_start TIMESTAMP(3),
	window_end TIMESTAMP(3),
	total_tip "numeric",
	PRIMARY KEY (window_start)
);