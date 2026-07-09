import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka_broker:29092") // FIX 1: correct broker hostname
  .option("subscribe", "customer-subscription")
  .option("startingOffsets", "latest") // FIX 2: latest for live demo
  .load()

val rawDF = kafkaDF.selectExpr("CAST(value AS STRING) as json_str")

val parsed = rawDF.select(
  get_json_object(col("json_str"), "$.customer.id").as("Id"),
  get_json_object(col("json_str"), "$.customer['first-name']").as("first_name"),   // FIX 3: bracket notation
  get_json_object(col("json_str"), "$.customer['last-name']").as("last_name"),     // FIX 3
  get_json_object(col("json_str"), "$.customer['legal-address'].city").as("city"), // FIX 3
  get_json_object(col("json_str"), "$.customer['legal-address']['postal-code']").as("postal_code"), // FIX 3
  get_json_object(col("json_str"), "$.customer['segmentation-category']").as("segmentation_category"), // FIX 3
  get_json_object(col("json_str"), "$.subscription.msisdn").as("msisdn")
).withColumn("wilaya", substring(col("postal_code"), 1, 2))
 .drop("postal_code")

def upsertToHudi(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
  val conf = batchDF.sparkSession.sparkContext.hadoopConfiguration
  conf.set("dfs.replication", "1")                                                 // FIX 4: HDFS replication
  conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")  // FIX 4
  batchDF.coalesce(1).write.format("hudi")                                         // FIX 5: coalesce
    .option("hoodie.table.name", "party")
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .option("hoodie.datasource.write.operation", "upsert")
    .option("hoodie.datasource.write.recordkey.field", "Id")
    .option("hoodie.datasource.write.precombine.field", "Id")
    .option("hoodie.datasource.write.partitionpath.field", "")
    .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
    .option("hoodie.datasource.hive_sync.enable", "false")
    .option("hoodie.upsert.shuffle.parallelism", "1")                              // FIX 6: parallelism 1
    .mode("append")
    .save("hdfs://docker-hive-namenode-1:8020/data/djezzy/silver/party")
}

val query = parsed.writeStream
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .option("checkpointLocation", "hdfs://docker-hive-namenode-1:8020/data/djezzy/checkpoints/party_streaming") // FIX 7: correct checkpoint path
  .foreachBatch(upsertToHudi _)
  .start()

query.awaitTermination()