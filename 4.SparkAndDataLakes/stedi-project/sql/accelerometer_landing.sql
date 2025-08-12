CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  user string,
  x double,
  y double,
  z double,
  timeStamp timestamp
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-sri-825/accelerometer/landing/';
