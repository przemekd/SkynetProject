// just drafting

// create table faa_master in hive firs

val faa_master = hiveContext.sql("select * from faa_master")
val adbs_09 = spark.read.json("hdfs:///data/adbs_exchange/2017-01-09")

faa_master.select("mode_s_code_hex").distinct.count
// res50: Long = 314697

adbs_09.select("Icao").distinct.count
res51: Long = 258700

adbs_09.createOrReplaceTempView("adbs_09")
faa_master.createOrReplaceTempView("faa_master")

// sheldon_adbs is a table containing transponder data from Sheldon

spark.sql("select distinct hexid from sheldon_adsb").collect.foreach(println)
// [4D0113]
// [392267]
// [A0FEA3]
// [4BA949]
// [A65C29]
// [A085A4]
// [45D92B]
// [A4D5F0]
// [89631D]
// [A732D9]
// [48411B]
// [A56DDC]

spark.sql("select distinct hexid, type_aircraft, name, n_number from sheldon_adsb s left join faa_master m on s.hexid=rtrim(m.mode_s_code_hex)").collect.foreach(println)
// [4D0113,null,null,null]
// [392267,null,null,null]
// [A0FEA3,5,UNITED PARCEL SERVICE CO                          ,163UP]
// [4BA949,null,null,null]
// [A65C29,5,JETBLUE AIRWAYS CORP                              ,509JB]
// [A085A4,5,WELLS FARGO BANK NORTHWEST NA TRUSTEE             ,13248]
// [45D92B,null,null,null]
// [A4D5F0,5,UNITED PARCEL SERVICE CO                          ,410UP]
// [89631D,null,null,null]
// [A732D9,5,WELLS FARGO BANK NORTHWEST NA TRUSTEE             ,563JB]
// [48411B,null,null,null]
// [A56DDC,5,UNITED PARCEL SERVICE CO                          ,449UP]

// So 50% of Sheldon transponder data can be matched by master reference file
// That's type matcher:
// 1 - Glider
// 2 - Balloon
// 3 - Blimp/Dirigible
// 4 - Fixed wing single engine 
// 5 - Fixed wing multi engine 
// 6 - Rotorcraft
// 7 - Weight-shift-control
// 8 - Powered Parachute
// 9 - Gyroplane

// now what about adsb exchange data
val matched = spark.sql("select distinct s.Icao, m.type_aircraft, m.name, m.n_number from adbs_09 s left join faa_master m on s.Icao=rtrim(m.mode_s_code_hex)").cache()
matched.count
// res73: Long = 258700

val matched_pos = matched.filter(matched("n_number").isNotNull).cache()
matched_pos.count
// res72: Long = 17039

// so less than 6.6% of adbsexchange data can be matched against master!
// that's pretty strange



// what do we save from adbs exchange
// Id
// Icao - mode s hexcode
// Bad
// Reg - tail number
// Call - call sign
// CallSus - call sign suspended
// Type - the aircraft's model ICAO type code [use this for type]
// Mdl - manufacturer's name
// CNum - constriction or serial number
// Op - aircraft's operator [not usable]
// OpCode - operators ICAO (actually OpIcao) [use this for airlines]
// Sqk - squawk
// Species - type of aircraft
// Mil - miliatry aircraft
// Cou - country
// Trt - transponder type
// Year - year of manufacturing

// to extract what we need :

val adbs_09 = spark.read.json("hdfs:///data/adbs_exchange/2017-01-09").filter($"Bad" === false)
val adbs_distinct_airplane_09 = spark.sql("select distinct Icao, Type, OpIcao from adbs_09").cache()
val adbs_grouped_by_type_and_airline_09 = adbs_distinct_airplane_09.groupBy("Type", "OpIcao").cache()
val day_09 = adbs_grouped_by_type_and_airline_09.count.orderBy($"count".desc).cache()
day_09.show

val masterDF = spark.sql("select n_number, mfr_mdl_code, mode_s_code_hex from faa_master")

val icaos_distinct = adbs_distinct_airplane_09.select("Icao").distinct
icaos_distinct.createOrReplaceTempView("icaos_distinct")
val joined = spark.sql("select n_number, mfr_mdl_code, mode_s_code_hex, Icao from masterDF m left join icaos_distinct i on i.Icao == trim(m.mode_s_code_hex)").cache()
joined.createOrReplaceTempView("joined")

joined.count
// res124: Long = 315314

joined.filter($"Icao".isNotNull).count
// res148: Long = 17039

val not_covered = joined.filter($"Icao".isNull)


//val rita_carrier = rita.select("tailnum", "uniquecarrier")
val rita_carrier = spark.sql("SELECT tailnum, uniquecarrier, max(flightdate) as date from rita group by tailnum, uniquecarrier").cache()

rita_carrier.createOrReplaceTempView("rita_carrier")

val rita_max = spark.sql("select max(date) as date, tailnum, 1 as matched from rita_carrier group by tailnum").cache()
rita_max.createOrReplaceTempView("rita_max")

val rita_max_joined = spark.sql("select m.matched, m.tailnum, m.date, c.uniquecarrier from rita_carrier c left join rita_max m on m.tailnum = c.tailnum and m.date = c.date").cache()
val unique_tailnums = rita_max_joined.filter($"matched".isNotNull)

val carrier_decode = hiveContext.sql("select * from carrier_decode")
carrier_decode.createOrReplaceTempView("carrier_decode")

unique_tailnums.createOrReplaceTempView("unique_tailnums")

// Dont do this
// val tailnum_to_carrier = spark.sql("select tailnum, u.uniquecarrier, c.unique_carrier_name from unique_tailnums u left join carrier_decode c on c.unique_carrier=u.uniquecarrier").cache()

val masterDF = spark.sql("select n_number, mfr_mdl_code, mode_s_code_hex, type_aircraft from faa_master")
masterDF.createOrReplaceTempView("masterDF")
val master_with_icao = spark.sql("select n_number, mfr_mdl_code, trim(mode_s_code_hex) as mode_s_code_hex, type_aircraft,Icao from masterDF m left join icaos_distinct i on i.Icao == trim(m.mode_s_code_hex)").cache()

val master_with_icao2 = spark.sql("select n_number, mfr_mdl_code, trim(mode_s_code_hex) as mode_s_code_hex, type_aircraft,Icao from masterDF m left join icaos_distinct i on i.Icao == trim(m.mode_s_code_hex)")

master_with_icao2.createOrReplaceTempView("master_with_icao2")

val master_enhanced = spark.sql("select n_number, mfr_mdl_code, mode_s_code_hex, type_aircraft, Icao, uniquecarrier, tailnum from master_with_icao2 m left join unique_tailnums u on u.tailnum = concat(\"N\", m.n_number)")

val master_airlines = master_enhanced.filter($"uniquecarrier".isNotNull).cache()

