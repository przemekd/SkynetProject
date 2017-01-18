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
