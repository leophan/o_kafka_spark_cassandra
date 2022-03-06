## Kafka - Spark Streaming - Cassandra

### Objective
* Kafka receives the messages from client
  * The data sends to kafka is JSON format
  * Format:
  ```json
    {
        "transaction_id": "de56408b-164e-4399-8a9d-3d27d7e26d4a",
        "created_date": "2022-03-05T02:33:44.146+07:00",
        "buyer_tax_id": "322872886",
        "buyer_id": "bid322872886@track",
        "delete_flag": "D",
        "bank_name": "BofA"
    }
    ```
* Spark Streaming transform data to store Cassandra database
  * If `bank_name` is `BofA` to store `bofa` table in database
  * If `bank_name` is `BofC` to store `bofc` table in database
* Cassandra has 2 tables
  * `bofa` is Bank of American
  * `bofc` is Bank of China

### Requirements
* Kafka_2.13-3.1.0
* Spark-3.2.1-bin-hadoop3.2
* Cassandra 4.0.3

#### Kafka
* Create a topic
```shell
kafka-topics.sh --create --topic bank-events --bootstrap-server localhost:9092
```
* Some commands
```shell
# detail topic
bin/kafka-topics.sh --describe --topic bank-events --bootstrap-server localhost:9092

# write some events into the topic
kafka-console-producer.sh --topic bank-events --bootstrap-server localhost:9092

# read the events
kafka-console-consumer.sh --topic bank-events --from-beginning --bootstrap-server localhost:9092
```

### Spark
* Dependence
```shell
# Work with kafka
commons-pool2-2.6.2.jar
kafka-clients-3.1.0.jar
kafka_2.12-3.1.0.jar
spark-sql-kafka-0-10_2.12-3.2.1.jar
spark-streaming-kafka-0-10_2.12-3.2.1.jar
spark-token-provider-kafka-0-10_2.12-3.2.1.jar

# Work with Cassandra
spark-cassandra-connector-assembly_2.12-3.1.0.jar
```
* Config
Copy all above library files into `jars` folder in `SPARK_HOME` folder
```shell
# example
cp commons-pool2-2.6.2.jar $SPARK_HOME/jars
```

#### Cassandra
* Create keyspace
```shell
CREATE KEYSPACE bank_poc
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    };
```
* Create table
```shell
# go to keyspace
USE bank_poc;

# Create `bofc` table
create table bofc
(
    transaction_id text primary key,
    created_date text,
    buyer_tax_id text,
    buyer_id text,
    delete_flag text,
    bank_name text
);

# Create `bofa` table
create table bofa
(
    transaction_id text primary key,
    created_date text,
    buyer_tax_id text,
    buyer_id text,
    delete_flag text,
    bank_name text
);
```

### Submit job
* Build `jar` file
```shell
# Build
cd o_hadoop_spark_scala
mvn package

# The `jar` file appears in `target` folder
```

* Submit
```shell
export ENV=dev

./bin/spark-submit \
--class uw.lab01.BofA \
--master spark://localhost:7077 \
./o_kafka_spark_cassandra/target/o_kafka_spark_cassandra-1.0-SNAPSHOT.jar
```

### Reference
* https://docs.microsoft.com/vi-vn/azure/cosmos-db/cassandra/spark-read-operation