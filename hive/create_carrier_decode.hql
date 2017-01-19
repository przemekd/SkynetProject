create external table carrier_decode (
AIRLINE_ID String,
CARRIER String,
CARRIER_ENTITY String,
CARRIER_NAME String,
UNIQUE_CARRIER String,
UNIQUE_CARRIER_ENTITY String,
UNIQUE_CARRIER_NAME String,
WAC String,
CARRIER_GROUP String,
CARRIER_GROUP_NEW String,
REGION String,
START_DATE_SOURCE String,
THRU_DATE_SOURC String
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/data/flightdata/carrier_decode/';
