package com.telekosmos.kafka.course

import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger

class ProducerDemo(logger: Logger) {

  val TOPIC = "scala-topic"

  import org.slf4j.Logger
  import org.slf4j.LoggerFactory

  // val logger: Logger = LoggerFactory.getLogger(classOf[Nothing])
  val props: Properties = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def justSend(msg: String): Unit = {
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String](TOPIC, msg)

    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, ex: Exception): Unit = {
        if (ex == null)
          logger.info("Metadata received :=> " + "Topic: " + metadata.topic + " - " + "Partition: " + metadata.partition + " - " + "Offset: " + metadata.offset)
        else
          logger.error("Producer error", ex)
      }
    })
    producer.flush()
    // producer.close()
  }

  def justClose(): Unit =  producer.close()
}
