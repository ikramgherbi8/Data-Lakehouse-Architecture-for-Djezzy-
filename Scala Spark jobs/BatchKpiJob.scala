import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

// Hardware Limits & Aggressive Partition Safety
spark.sparkContext.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false")
spark.sparkContext.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
spark.conf.set("spark.sql.shuffle.partitions", "4")

// Load Staging Tables
val egsn = spark.table("djezzy_staging.stg_egsn")
val egsn5g = spark.table("djezzy_staging.stg_egsn_5g")
val ofr = spark.table("djezzy_staging.stg_ofr")
val cellSite = spark.table("djezzy_staging.stg_cell_site")

// Clean Lookups
val ofrLookup = ofr.withColumn("ofr_id", regexp_replace(col("ofr_id"), "\"", "")).withColumn("line_type", regexp_replace(col("line_type"), "\"", "")).withColumn("main_tp", regexp_replace(col("main_tp"), "\"", "")).withColumn("tariff_profile", regexp_replace(col("tariff_profile"), "\"", "")).select("ofr_id", "line_type", "main_tp", "tariff_profile").filter(col("ofr_id") =!= "OFR_ID" && col("ofr_id") =!= "").coalesce(1)

val cellLookup = cellSite.select(col("id"), col("region"), col("wilaya"), col("commune"), col("longitude").cast("float"), col("altitude").cast("float").alias("latitude")).filter(col("id") =!= "ID").coalesce(1)

val subs5g = egsn.filter(col("timeoffirstusage") =!= "timeOfFirstUsage" && col("timeoffirstusage").isNotNull).select(col("servedimsi")).distinct.coalesce(2)

// Streamlined Unrolling Plan
val uplinkCols = (0 to 49).map(i => coalesce(regexp_replace(col(s"datavolumeuplink$i"), ",00", "").cast("bigint"), lit(0L))).reduce(_ + _)
val downlinkCols = (0 to 49).map(i => coalesce(regexp_replace(col(s"datavolumedownlink$i"), ",00", "").cast("bigint"), lit(0L))).reduce(_ + _)

// Build Enriched Base Sets
val egsn5gBase = {
  val base = egsn5g.filter(col("offering_id") =!= "OFFERING_ID" && col("offering_id").isNotNull).filter(col("file_arriving_date") =!= "File_Arriving_Date").join(broadcast(ofrLookup), egsn5g("offering_id") === ofrLookup("ofr_id"), "inner").filter(col("line_type").isin("3G", "4G", "5G")).withColumn("date", lit("2026-06-01")).withColumn("uplink_vol", uplinkCols).withColumn("downlink_vol", downlinkCols)
  base.join(broadcast(subs5g.withColumnRenamed("servedimsi", "imsi_5g")), base("servedimsi") === col("imsi_5g"), "left").withColumn("line_type", when(col("imsi_5g").isNotNull, lit("5G")).otherwise(col("line_type"))).drop("imsi_5g")
}

val egsn5gEnriched = egsn5gBase.join(broadcast(cellLookup), egsn5gBase("cell_id") === cellLookup("id"), "left").drop("id")

val egsnBase = egsn.filter(col("timeoffirstusage") =!= "timeOfFirstUsage" && col("timeoffirstusage").isNotNull).join(broadcast(ofrLookup), egsn("offering_id") === ofrLookup("ofr_id"), "inner").withColumn("uplink_vol", regexp_replace(col("datavolumefbcuplink"), "\\.00$", "").cast("bigint")).withColumn("downlink_vol", regexp_replace(col("datavolumefbcdownlink"), "\\.00$", "").cast("bigint")).withColumn("date", lit("2026-06-01"))

val egsnEnriched = egsnBase.join(broadcast(cellLookup), egsnBase("cell_id") === cellLookup("id"), "left").drop("id")

// Memory Optimization: Using low-cost aggregations instead of expensive analytical windows
val baseAgg5g = egsnEnriched.groupBy("date", "servedimsi").agg(sum("uplink_vol").alias("uplink_5g"), sum("downlink_vol").alias("downlink_5g"), first("main_tp").alias("main_tp"), first("tariff_profile").alias("tariff_profile"), first("line_type").alias("line_type"))

val topGeo5g = egsnEnriched.filter(col("region").isNotNull).groupBy("date", "servedimsi", "region", "wilaya", "commune", "longitude", "latitude").count().repartition(col("date"), col("servedimsi")).sortWithinPartitions(col("count").desc).groupBy("date", "servedimsi").agg(first("region").alias("region"), first("wilaya").alias("wilaya"), first("commune").alias("commune"), first("longitude").alias("longitude"), first("latitude").alias("latitude"))

val agg5g = baseAgg5g.join(topGeo5g, Seq("date", "servedimsi"), "left").as("df5g")

val baseAggLegacy = egsn5gEnriched.groupBy("date", "servedimsi", "line_type").agg(sum("uplink_vol").alias("uplink_legacy"), sum("downlink_vol").alias("downlink_legacy"), first("main_tp").alias("main_tp"), first("tariff_profile").alias("tariff_profile")).withColumn("uplink_5g_from_legacy", when(col("line_type") === "5G", col("uplink_legacy")).otherwise(lit(0L))).withColumn("downlink_5g_from_legacy", when(col("line_type") === "5G", col("downlink_legacy")).otherwise(lit(0L))).withColumn("uplink_4g", when(col("line_type") =!= "5G", col("uplink_legacy")).otherwise(lit(0L))).withColumn("downlink_4g", when(col("line_type") =!= "5G", col("downlink_legacy")).otherwise(lit(0L))).groupBy("date", "servedimsi").agg(sum("uplink_5g_from_legacy").alias("uplink_5g_from_legacy"), sum("downlink_5g_from_legacy").alias("downlink_5g_from_legacy"), sum("uplink_4g").alias("uplink_4g"), sum("downlink_4g").alias("downlink_4g"), first("main_tp").alias("main_tp"), first("tariff_profile").alias("tariff_profile"), first("line_type").alias("line_type"))

val topGeoL = egsn5gEnriched.filter(col("region").isNotNull).groupBy("date", "servedimsi", "region", "wilaya", "commune", "longitude", "latitude").count().repartition(col("date"), col("servedimsi")).sortWithinPartitions(col("count").desc).groupBy("date", "servedimsi").agg(first("region").alias("region"), first("wilaya").alias("wilaya"), first("commune").alias("commune"), first("longitude").alias("longitude"), first("latitude").alias("latitude"))

val aggLegacy = baseAggLegacy.join(topGeoL, Seq("date", "servedimsi"), "left").as("dfLegacy")

// Full KPI Consolidation
val kpiTable = {
  val joined = agg5g.join(aggLegacy, Seq("date", "servedimsi"), "full")
  joined.withColumn("vol_5g", coalesce(col("df5g.uplink_5g"), lit(0L)) + coalesce(col("df5g.downlink_5g"), lit(0L)) + coalesce(col("dfLegacy.uplink_5g_from_legacy"), lit(0L)) + coalesce(col("dfLegacy.downlink_5g_from_legacy"), lit(0L)))
    .withColumn("vol_4g", coalesce(col("dfLegacy.uplink_4g"), lit(0L)) + coalesce(col("dfLegacy.downlink_4g"), lit(0L)))
    .withColumn("record_key", concat_ws("_", col("date"), col("servedimsi")))
    .select(
      col("date"), 
      col("servedimsi"), 
      coalesce(col("df5g.line_type"), col("dfLegacy.line_type")).alias("line_type"), 
      col("vol_4g"), 
      col("vol_5g"), 
      col("record_key"), 
      coalesce(col("df5g.main_tp"), col("dfLegacy.main_tp"), lit("UNKNOWN")).alias("main_tp"), 
      coalesce(col("df5g.tariff_profile"), col("dfLegacy.tariff_profile"), lit("UNKNOWN")).alias("tariff_profile"), 
      coalesce(col("df5g.region"), col("dfLegacy.region")).alias("region"), 
      coalesce(col("df5g.wilaya"), col("dfLegacy.wilaya")).alias("wilaya"), 
      coalesce(col("df5g.commune"), col("dfLegacy.commune")).alias("commune"), 
      coalesce(col("df5g.longitude"), col("dfLegacy.longitude")).alias("longitude"), 
      coalesce(col("df5g.latitude"), col("dfLegacy.latitude")).alias("latitude")
    )
}

// 8. Hudi Target Parameters - Memory-Throttled Configuration
val hudiOptions = Map(
  "hoodie.table.name" -> "kpi_daily_subscriber", 
  "hoodie.datasource.write.recordkey.field" -> "record_key", 
  "hoodie.datasource.write.precombine.field" -> "date", 
  "hoodie.datasource.write.partitionpath.field" -> "date", 
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ", 
  "hoodie.datasource.write.operation" -> "upsert", 
  "hoodie.insert.shuffle.parallelism" -> "2", 
  "hoodie.upsert.shuffle.parallelism" -> "2",
  "hoodie.write.markers.type" -> "DIRECT",
  "hoodie.memory.merge.fraction" -> "0.20",
  "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
  "hoodie.datasource.hive_sync.enable" -> "true", 
  "hoodie.datasource.hive_sync.database" -> "djezzy_silver", 
  "hoodie.datasource.hive_sync.table" -> "kpi_daily_subscriber", 
  "hoodie.datasource.hive_sync.jdbcurl" -> "jdbc:hive2://hive-server:10000", 
  "hoodie.datasource.hive_sync.partition_fields" -> "date", 
  "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor"
)

// Execute Write
kpiTable.write.format("org.apache.hudi").options(hudiOptions).mode(SaveMode.Append).save("hdfs://docker-hive-namenode-1:8020/data/djezzy/silver/kpi_daily_subscriber")

println("=== KPI Table Saved Successfully ===")