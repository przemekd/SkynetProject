# Run that script from root dir!!
mkdir -p data/
curl -o data/ReleasableAircraft.zip http://registry.faa.gov/database/ReleasableAircraft.zip

cd data/
unzip ReleasableAircraft.zip

hdfs dfs -mkdir /data/flightdata/faa
hdfs dfs -mkdir /data/flightdata/faa/master
hdfs dfs -mkdir /data/flightdata/faa/aircraft_references

hdfs dfs -copyFromLocal MASTER.txt  /data/flightdata/faa/master
hdfs dfs -copyFromLocal ACFTREF.txt /data/flightdata/faa/aircraft_references
