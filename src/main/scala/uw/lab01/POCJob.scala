package uw.lab01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import uw.lab01.Utils._

object POCJob {

  def main(args: Array[String]): Unit = {
    val env = sys.env("ENV")
    val kafkaHost = "localhost"
    val sparkMasterHost = "127.0.0.1"
    val cassandraHost = "127.0.0.1"
    val cassandraPort = "9042"
    val cassandraUsername = "cassandra"
    val cassandraPassword = "cassandra"
    val keyspace = "bank_poc"
    val table_bank_of_american = "bofa"
    val table_bank_of_china = "bofc"
    val checkpointLocation = "/tmp/checkpoint/"

    val bank_schema = new StructType()
      .add("transaction_id", StringType)
      .add("created_date", StringType)
      .add("buyer_tax_id", StringType)
      .add("buyer_id", StringType)
      .add("delete_flag", StringType)
      .add("bank_name", StringType)

    val spark = init(SparkSession, "Kafka-Spark-Cassandra-POC", env)
    spark.conf.set("spark.cassandra.connection.host", cassandraHost)
    spark.conf.set("spark.cassandra.connection.port", cassandraPort)
    spark.conf.set("spark.cassandra.auth.username", cassandraUsername)
    spark.conf.set("spark.cassandra.auth.password", cassandraPassword)
    spark.sparkContext.setLogLevel("ERROR")

    val event_df = load(spark, kafkaHost)                           // load data from Kafka
    val clean_df = clean(event_df, bank_schema)                     // clean data

    val bank_bofa_df = filter(clean_df, "BofA", 1)    // filter data if bank_name is BofA
    val bank_bofc_df = filter(clean_df, "BofC", 1)    // filter data if bank_name is BofC

    val cassandra_bofa_df = store(bank_bofa_df,
      table_bank_of_american,
      keyspace,
      s"$checkpointLocation/$table_bank_of_american/") // bofa dataframe writes to cassandra
    val cassandra_bofc_df = store(bank_bofc_df,
      table_bank_of_china,
      keyspace,
      s"$checkpointLocation/$table_bank_of_china/")    // bofc dataframe writes to cassandra

    cassandra_bofa_df.awaitTermination()                                          // wait for the execution to stop
    cassandra_bofc_df.awaitTermination()

  }

}
