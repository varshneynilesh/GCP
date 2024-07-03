CREATE TABLE `<PROJECT_ID>.<DATASET>.realtime`
(
  ride_id STRING,
  point_idx INT64,
  latitude FLOAT64,
  longitude FLOAT64,
  timestamp TIMESTAMP,
  meter_reading FLOAT64,
  meter_increment FLOAT64,
  ride_status STRING,
  passenger_count INT64
)
PARTITION BY DATE(timestamp);
