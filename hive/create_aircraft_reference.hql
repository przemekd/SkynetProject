create external table faa_aircraft_reference (
    CODE String,
    MFR String,
    MODEL String,
    TYPE_ACFT String,
    TYPE_ENG String,
    AC_CAT String,
    BUILD_CERT_IND String,
    NO_ENG String,
    NO_SEATS String,
    AC_WEIGHT String,
    SPEED String
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/data/flightdata/aircraft-registrations/';
