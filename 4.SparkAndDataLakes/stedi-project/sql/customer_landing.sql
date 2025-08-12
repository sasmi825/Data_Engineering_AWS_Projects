CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  serialnumber string,
  sharewithpublicasofdate timestamp,
  birthday date,
  registrationdate timestamp,
  sharewithresearchasofdate timestamp,
  customername string,
  email string,
  lastupdatedate timestamp,
  phone string,
  sharewithfriendsasofdate timestamp
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-sri-825/customer/landing/';
