package uw.lab01

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {


  def init(sparkSession: SparkSession.type , name: String, env: String): SparkSession = {
    if (env == "local") {
      val spark = sparkSession.builder()
        .master("local[2]")
        .appName("Kafka-Spark-Cassandra-POC")
        .getOrCreate()
      spark
    } else {
      val spark = sparkSession.builder()
        .appName("Kafka-Spark-Cassandra-POC")
        .getOrCreate()
      spark
    }
  }

  def load(spark: SparkSession, kafkaHost: String): DataFrame = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${kafkaHost}:9092")
      .option("subscribe", "bank-events")
      .option("startingOffsets", "latest") // read at latest position in topic kafka
      .load()
    df
  }

  def clean(df: DataFrame, schema: StructType) = {
    val event_df = df.selectExpr("CAST(value AS STRING)") // cast value to string

    val bank_df = event_df
      .select(from_json(col("value"), schema).as("data")) // parse json based on schema
      .select("data.*")

    bank_df
  }

  def filter(df: DataFrame, name: String, debug: Int = 0) = {
    val bank_df = df.filter(df("bank_name") === name)
    if (debug == 1) {
      bank_df.printSchema()
    }
    bank_df
  }

  def store(df: DataFrame, table: String, keyspace: String, checkpointLocation: String, debug: Int = 0) = {
    if (debug == 1) {
      val cassandra_bofc_df = df.writeStream // write data to console, it's used to debug
        .format("console")
        .outputMode("append")
        .start()
      cassandra_bofc_df
    } else {
      val cassandra_bofc_df = df.writeStream // write data into cassandra database
        .option("checkpointLocation", checkpointLocation)
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> table, "keyspace" -> keyspace))
        .start()
      cassandra_bofc_df
    }
  }

}
