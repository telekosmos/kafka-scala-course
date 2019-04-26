package com.telekosmos.kafka.course

import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.{Logger, LoggerFactory}

class Reprocessor(props: Properties, logger: Logger, latch: CountDownLatch) extends Runnable {
  val TOPIC = "scala-topic"

  private val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

  override def run(): Unit = {
    var keepGoing = true
    var numMsgs = 0
    var countdown = false
    try {
      while (keepGoing) { // !Thread.currentThread.isInterrupted) {
        logger.info(s"-----------------------------> $keepGoing")
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(5000))
        logger.info(s"***==-=> Read ${records.count()} messages")
        records.forEach(r => logger.info("key: " + r.key + ", val: " + r.value + ", part: " + r.partition + ", offset: " + r.offset))
        logger.info("xxxxxxxxxxxxxxx*xxxxxxxxxxxxxxx")

        numMsgs += records.count()
        keepGoing = records.count() == 0
      }
    }
    catch {
      case x: Throwable => {
        logger.info(s"Unknown exception: ${x.getMessage}")
        throw x
      }
    }
    finally {
      logger.info(s"@@@ App (reprocessor) is closing: Read $numMsgs messages")
      if (!countdown)
        latch.countDown()

      consumer.close()
      logger.info("@@@ Reprocessor closed")
    }
  }

  def shutdown(): Unit = consumer.wakeup()

  def setPartitionOffset(partitionNumber:Int, offset:Long): Unit = {
    val tp:TopicPartition = new TopicPartition(TOPIC, partitionNumber)
    consumer.assign(java.util.Arrays.asList(tp))
    consumer.seek(tp, offset)
  }

}
