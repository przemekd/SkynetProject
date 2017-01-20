// How did we process adsb data
// val adbs_09 = spark.read.json("hdfs:///data/adbs_exchange/2017-01-09").filter($"Bad" === false)
// val adbs_distinct_airplane_09 = spark.sql("select distinct Icao, Type, OpIcao from adbs_09").cache()
// adbs_distinct_airplane_09.write.format("csv").save("hdfs:///data/adbs_extracted")
// Takes too long to show on demo

// The final process
// load all tables from hive

// FAA database
spark.table("skynet.faa_master").write.saveAsTable("faa_master")
spark.table("skynet.faa_aircraft_reference").write.saveAsTable("faa_aircraft_reference")

// transponders data
spark.table("skynet.adsb_extract").write.saveAsTable("adsb")

// rita support data
spark.table("skynet.carrier_decode").write.saveAsTable("carrier_decode")

// limit master faa data
val master_limited = spark.sql("select n_number, mfr_mdl_code, trim(mode_s_code_hex) as mode_s_code_hex, type_aircraft from faa_master")
master_limited.createOrReplaceTempView("master_limited")

// get distinct airplanes from adsb data
val adsb_distinct_airplanes = spark.sql("select distinct icao from adsb")
adsb_distinct_airplanes.createOrReplaceTempView("adsb_distinct_airplanes")

// join master with adsb and remove covered aircrafts
val master_with_adsb = spark.sql("select n_number, mfr_mdl_code, mode_s_code_hex, type_aircraft,Icao from master_limited m left join adsb_distinct_airplanes i on i.Icao == m.mode_s_code_hex")
val master_not_covered = master_with_adsb.filter($"icao".isNull)
master_not_covered.createOrReplaceTempView("master_not_covered")

// !!!STAGE 1!!!
// master_not_covered.write.format("parquet").save("hdfs:///data/master_not_covered")
// val master_not_covered = spark.read.parquet("hdfs:///data/master_not_covered")
// master_not_covered.createOrReplaceTempView("master_not_covered")
// master_not_covered.cache()
// master_not_covered.collect
// !!!STAGE 1!!!

// read RITA on time data and extract tailnumbers and unique carrier codes
val rita = spark.read.parquet("hdfs:///data/flightdata/parquet-trimmed")
rita.createOrReplaceTempView("rita")
val rita_carrier = spark.sql("SELECT tailnum, uniquecarrier, max(flightdate) as date from rita group by tailnum, uniquecarrier")
rita_carrier.createOrReplaceTempView("rita_carrier")

val rita_max = spark.sql("select max(date) as date, tailnum, 1 as matched from rita_carrier group by tailnum")
rita_max.createOrReplaceTempView("rita_max")

val rita_max_and_carrier = spark.sql("select m.matched, m.tailnum, m.date, c.uniquecarrier from rita_carrier c left join rita_max m on m.tailnum = c.tailnum and m.date = c.date")
val rita_unique_tailnums = rita_max_and_carrier.filter($"matched".isNotNull)
rita_unique_tailnums.createOrReplaceTempView("rita_unique_tailnums")

// !!! STAGE 2 !!!
// rita_unique_tailnums.write.format("parquet").save("hdfs:///data/rita_unique_tailnums")
// val rita_unique_tailnums = spark.read.parquet("hdfs:///data/rita_unique_tailnums")
// rita_unique_tailnums.createOrReplaceTempView("rita_unique_tailnums")
// !!! STAGE 2 !!!

// combine everything in a single table
val master_enhanced = spark.sql("select n_number, mfr_mdl_code, mode_s_code_hex, type_aircraft, Icao, uniquecarrier, tailnum from master_not_covered m left join rita_unique_tailnums u on u.tailnum = concat(\"N\", m.n_number)")

// drop all aircraft that are not owned by airlines
val master_interesting = master_enhanced.filter($"uniquecarrier".isNotNull)
master_interesting.createOrReplaceTempView("master_interesting")

// !!! STAGE 3 !!!
// master_interesting.write.format("parquet").save("hdfs:///data/master_interesting")
// val master_interesting = spark.read.parquet("hdfs:///data/master_interesting")
// master_interesting.createOrReplaceTempView("master_interesting")
// !!! STAGE 3 !!!

// master_interesting.write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///data/master_interesting_csv")

// rational of our approach
spark.sql("select type_aircraft, count(*) as cnt from master_interesting group by type_aircraft").show
// +-------------+---+
// |type_aircraft|cnt|
// +-------------+---+
// |            7|  5|
// |            5|799|
// |            6| 10|
// |            1|  2|
// |            4| 39|
// |            2|  1|
// +-------------+---+

// group airplanes by model (first 5 digits)
val master_grouped = spark.sql("select substr(mfr_mdl_code, 0, 5), count(*) as cnt from master_interesting group by substr(mfr_mdl_code, 0, 5) order by cnt DESC")
master_grouped.collect.foreach(println)
// [13900,159]
// [13849,138]
// [13844,111]
// [30220,95]
// [32602,70]
// [19003,33]
// [13851,27]
// [13852,27]
// [39303,26]
// [39400,24]
// [13837,21]
// [28090,12]
// [30236,8]
// [05658,8]
// [13845,8]
// [32603,6]
// [13840,4]
// [20724,4]
// ...

// analyze most popular codes
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"13900\"").show
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"13849\"").show
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"13844\"").show
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"30220\"").show
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"32602\"").show


// airlines names matching
val unique_carrier_decode = spark.sql("select distinct unique_carrier, unique_carrier_name from carrier_decode")
unique_carrier_decode.createOrReplaceTempView("unique_carrier_decode")

val master_with_airlines_names = spark.sql("select u.unique_carrier_name, m.uniquecarrier, m.mfr_mdl_code, m.mode_s_code_hex, m.type_aircraft, m.tailnum from master_interesting m left join unique_carrier_decode u on u.unique_carrier = m.uniquecarrier")
master_with_airlines_names.createOrReplaceTempView("master_with_airlines_names")

// match with cleaned types
val faa_aircraft_reference = spark.sql("select code, mfr, model from faa_aircraft_reference")

def cleanTypes(model: String): String = {
if (model == null) {
model
}
else if ( ( "EMB.*".r findAllIn model ).length > 0 ) {
val r = "(EMB-[0-9]{3}).*".r 
model match {
case r(group) => group
case _ => model
}
}
else if ( ("7[0-9]7".r findAllIn model).length > 0 ) {
val r = "(7[0-9]7).*".r 
model match {
case r(group) => group
case _ => model
}
}
else if ( ("DC-9".r findAllIn model).length > 0 ) {
val r = "(DC-9).*".r 
model match {
case r(group) => group
case _ => model
}
}
else if ( model == "DC9-15" ) {
"DC-9"
}
else if ( ("DHC-8".r findAllIn model).length > 0 ) {
val r = "(DHC-8).*".r 
model match {
case r(group) => group
case _ => model
}
}
else if ( ("CL-60".r findAllIn model).length > 0 ) {
val r = "(CL-[0-9]{3}).*".r 
model match {
case r(group) => group
case _ => model
}
}
else {
model
}
}

spark.udf.register("cleanTypes", cleanTypes _)

val cleaned_types = spark.sql("select code, cleanTypes(model) as model from faa_aircraft_reference")
cleaned_types.createOrReplaceTempView("cleaned_types")

val master_final = spark.sql("select m.unique_carrier_name, m.uniquecarrier, m.mfr_mdl_code, m.mode_s_code_hex, m.type_aircraft, m.tailnum, t.model from master_with_airlines_names m join cleaned_types t on t.code = m.mfr_mdl_code")

master_final.createOrReplaceTempView("master_final")

// !!! STAGE 4 !!!
// master_final.write.format("parquet").save("hdfs:///data/master_final")
// val master_final = spark.read.parquet("hdfs:///data/master_final")
// master_final.createOrReplaceTempView("master_final")
// !!! STAGE 4 !!!

// Get airplane types
spark.sql("select model, count(*) as cnt from master_final group by model order by cnt DESC").collect.foreach(println)
// [CL-600,189]
// [757,124]
// [737,120]
// [DC-9,95]
// [767,46]
// [EMB-145,38]
// [EMB-120,32]

// Group by airlines
spark.sql("select model, unique_carrier_name, count(*) as cnt from master_final where (model = \"CL-600\" or model = \"737\" or model = \"757\") group by model, unique_carrier_name order by cnt DESC").collect.foreach(println)
// [737,Southwest Airlines Co.,80]
// [CL-600,Endeavor Air Inc.,75]
// [757,Delta Air Lines Inc.,68]
// [CL-600,ExpressJet Airlines Inc.,42]
// [CL-600,SkyWest Airlines Inc.,37]
// [757,United Air Lines Inc.,34]
// [CL-600,PSA Airlines Inc.,25]
// [757,American Airlines Inc.,21]
// [737,Alaska Airlines Inc.,17]
// [737,US Airways Inc.,14]
// [CL-600,Mesa Airlines Inc.,10]
// [737,Delta Air Lines Inc.,4]
// [737,United Air Lines Inc.,4]
// [737,AirTran Airways Corporation,1]
// [757,AirTran Airways Corporation,1]
