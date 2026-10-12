import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

// Read from Hive staging table
val df = spark.table("djezzy_staging.stg_cell_site")
println(s"Row count from staging: ${df.count()}")
df.printSchema()

// Add required Hudi meta fields
val hudiDf = df.withColumn("ts", lit(System.currentTimeMillis()))

// Hudi options
val hudiOptions = Map(
  "hoodie.table.name" -> "test_cell_site",
  "hoodie.datasource.write.recordkey.field" -> "cell_site_id",
  "hoodie.datasource.write.partitionpath.field" -> "",
  "hoodie.datasource.write.precombine.field" -> "ts",
  "hoodie.datasource.write.operation" -> "upsert",
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.datasource.hive_sync.enable" -> "false"
)

// Write to HDFS as Hudi table
hudiDf.write
  .format("org.apache.hudi")
  .options(hudiOptions)
  .mode(SaveMode.Overwrite)
  .save("hdfs://namenode:8020/data/djezzy/hudi/test_cell_site")

println("Hudi write succeeded!")

// Read back
val readDf = spark.read
  .format("org.apache.hudi")
  .load("hdfs://namenode:8020/data/djezzy/hudi/test_cell_site")

println(s"Row count from Hudi: ${readDf.count()}")
