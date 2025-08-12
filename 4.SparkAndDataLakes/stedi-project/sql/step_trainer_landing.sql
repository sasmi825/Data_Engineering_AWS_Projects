CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorReadingTime timestamp,
  serialNumber string,
  distanceFromObject double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-sri-825/step_trainer/landing/';

