# Run that script from root dir!!
mkdir -p data/
#curl -o data/ReleasableAircraft.zip http://registry.faa.gov/database/ReleasableAircraft.zip
cd data/
unzip ReleasableAircraft.zip

hdfs dfs -copyFromLocal MASTER.txt /data/flightdata/aircraft-registrations/
