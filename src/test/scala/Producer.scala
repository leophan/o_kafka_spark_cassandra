import org.apache.kafka.clients.producer._
import org.joda.time.DateTime

import java.util.Properties
import java.util.UUID.randomUUID

object Producer {
  /**
   * The function will run until it is stopped.
   * @param args
   */
  def main(args: Array[String]): Unit = {
    writeToKafka("bank-events")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    while (true) {
      val transactionId = randomUUID()                                      // randomUUID such as 123e4567-e89b-12d3-a456-426614174000
      val createdDate = DateTime.now()                                      // get datetime
      val r = scala.util.Random                                             // random function
      val buyerTaxId = r.nextInt(665880748)                                 // random number between 0 and 665880748
      val deleteFlag = if (r.nextInt(10) % 2 == 0) "D" else "N"             // if random value % 2 equal 0 deleteFlag gets "D", else "N"
      val bankName = if (r.nextInt(665880748) % 2 == 0) "BofC" else "BofA"  // if random value % 2 equal 0 bankName gets "BofC", else "BofA"
      val msg = f"""{"transaction_id":"$transactionId","created_date":"$createdDate","buyer_tax_id":"$buyerTaxId","buyer_id":"bid$buyerTaxId@track","delete_flag":"$deleteFlag","bank_name":"$bankName"}"""
      val record = new ProducerRecord[String, String](topic, msg)           // prepare message of kafka
      producer.send(record)                                                 // send message to kafka
      println(f"Sending $msg")
      Thread.sleep(5000)                                              // just wait to make sure we can monitor msg on consumer console
    }
    producer.close()
  }
}