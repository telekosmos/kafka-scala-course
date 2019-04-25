package com.telekosmos.kafka.course

import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.{Logger, LoggerFactory}

class ConsumerThread(props: Properties, logger: Logger, latch: CountDownLatch) extends Runnable {
  val TOPIC = "scala-topic"

  private var running = true
  // private val logger = LoggerFactory.getLogger(classOf[ConsumerThread])
  private val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

  println(s"Constructing ${classOf[ConsumerThread]}")

  override def run(): Unit = {
    // Create consumer from props
    consumer.subscribe(Collections.singletonList(TOPIC))
    logger.info(s"Just subscribed to $TOPIC")

    try {
      while (running) { // !Thread.currentThread.isInterrupted) {
        logger.info(s"-----------------------------> $running")
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(5000))
        logger.info(s"***==-=> Read ${records.count()} messages")
        records.forEach(r => logger.info("key: " + r.key + ", val: " + r.value + ", part: " + r.partition + ", offset: " + r.offset))
        logger.info("xxxxxxxxxxxxxxx*xxxxxxxxxxxxxxx")
      }
    }
    catch {
      case wex: WakeupException => {
        logger.info("Received wakeup signal to shutdown")
        latch.countDown()
        // Thread.currentThread.interrupt()
        // throw wex
      }
      case x: Throwable => {
        logger.info(s"Unknown exception: ${x.getMessage}")
        throw x
      }
    }
    finally {
      logger.info("@@@ App (consumer) is closing...")
      running = !running
      consumer.close()
      logger.info("@@@ Consumer closed")
    }
  }

  def shutdown(): Unit = consumer.wakeup()

}
