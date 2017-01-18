create external table adsb_extract (
    Icao STRING,
    Type STRING,
    OpIcao STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/data/flightdata/adsb/extract';