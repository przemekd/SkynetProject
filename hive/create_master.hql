create external table faa_master (
    N_NUMBER String,
    SERIAL_NUMBER String,
    MFR_MDL_CODE String,
    ENG_MFR_MDL String,
    YEAR_MFR String,
    TYPE_REGISTRANT String,
    NAME String,
    STREET String,
    STREET2 String,
    CITY String,
    STATE String,
    ZIP_CODE String,
    REGION String,
    COUNTY String,
    COUNTRY String,
    LAST_ACTION_DATE String,
    CERT_ISSUE_DATE String,
    CERTIFICATION String,
    TYPE_AIRCRAFT String,
    TYPE_ENGINE String,
    STATUS_CODE String,
    MODE_S_CODE String,
    FRACT_OWNER String,
    AIR_WORTH_DATE String,
    OTHER_NAMES1 String,
    OTHER_NAMES2 String,
    OTHER_NAMES3 String,
    OTHER_NAMES4 String,
    OTHER_NAMES5 String,
    EXPIRATION_DATE String,
    UNIQUE_ID String,
    KIT_MFR String,
    KIT_MODEL String,
    MODE_S_CODE_HEX String)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/data/flightdata/aircraft-registrations/MASTER';
