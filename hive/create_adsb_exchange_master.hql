CREATE EXTERNAL TABLE adsb_exchange_master(
lastDv BIGINT,
totalAc INT,
src INT,
showSil BOOLEAN,
showFlg BOOLEAN,
showPic BOOLEAN,
flgW INT,
flgH INT,
shtTrlSec INT,
stm BIGINT,
acList ARRAY<STRING>,
feeds ARRAY<STRING>,
srcFeed INT,
configChanged BOOLEAN)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
PARTITIONED BY (date string)
STORED AS TEXTFILE
LOCATION '/data/flightdata/adsb/';