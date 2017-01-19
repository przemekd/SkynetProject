// How did we process adsb data
//
//
//

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

// combine everything in a single table
val master_enhanced = spark.sql("select n_number, mfr_mdl_code, mode_s_code_hex, type_aircraft, Icao, uniquecarrier, tailnum from master_not_covered m left join rita_unique_tailnums u on u.tailnum = concat(\"N\", m.n_number)")

// drop all aircraft that are not owned by airlines
val master_interesting = master_enhanced.filter($"uniquecarrier".isNotNull)
master_interesting.createOrReplaceTempView("master_interesting")

master_interesting.write.format("com.databricks.spark.csv").option("header", "true").save("file:///vagrant/data/master_interesting")

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
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"13900\"")
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"13849\"")
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"13844\"")
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"30220\"")
spark.sql("select code, mfr, model from faa_aircraft_reference where substr(code,0,5) =\"32602\"")



