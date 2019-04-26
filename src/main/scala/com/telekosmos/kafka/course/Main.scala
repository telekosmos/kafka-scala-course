package com.telekosmos.kafka.course

import java.text.SimpleDateFormat
import java.util.concurrent.CountDownLatch
import java.util._

import com.telekosmos.kafka.course.util.Utils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.List

object Main extends App {
  private def dateStr(d: Date): String = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss").format(d)

  // (new ProducerDemo).justSend(s"""{"value": "Yet another message", "ts": "$dateStr"}""")
  private def timeStr(d: Date): String = new SimpleDateFormat("HH:mm:ss").format(d)

  private def logger: Logger = LoggerFactory.getLogger(classOf[ConsumerThread])

  override def main(args: Array[String]) = {
    val numConsumers = Utils.getNumOfConsumers(args) getOrElse 0
    val numProducers = Utils.getNumOfProducers(args) getOrElse 0
    val numReprocessors = Utils.getReprocessors(args) getOrElse 0
    logger.info(s"CLI args: consumers $numConsumers, producers $numProducers")

    val latch: CountDownLatch = new CountDownLatch(numConsumers + numProducers + numReprocessors)

    logger.info(s"Starting ${numConsumers} consumers")
    val consumerThreads: List[ConsumerThread] = List.fill(numConsumers)(runConsumer(logger, latch))

    logger.info(s"Starting $numProducers producers")
    val producers: List[ProducerDemo] = List.range(1, numProducers + 1).map(p => runProducer(p * 10, 3))

    logger.info(s"Starting $numReprocessors reprocessors")
    def modN(n: Int)(x: Int) = n % x
    def partitionNum(x:Int) = modN(x)(3)
    val reprocessors: List[Reprocessor] = List
      .range(1, numReprocessors+1)
      .map(r => runReprocessor(logger, latch, partitionNum(r), 1))

    sys.addShutdownHook({
      println("@@@ Entering shutdownhook")
      // runnable.asInstanceOf[ConsumerThread].shutdown()
      // consumerThread.shutdown()
      logger.info(s"Shuutting down ${consumerThreads.size} consumers")
      consumerThreads.foreach(c => c.shutdown())
      producers.foreach(p => p.justClose())
      reprocessors.foreach(r => r.shutdown())
    })

    try {
      logger.info("@@@ Awaiting for latch")
      latch.await()
    } catch {
      case e: InterruptedException => logger.info(s"Application got interrupted: $e")
    } finally {
      logger.info("Application is closing")
    }
  }

  def runProducer(n: Int, factor: Int): ProducerDemo = {
    logger.info("### Running producer...")
    val producer = new ProducerDemo(logger)
    val deltaInMillis: Int = 2500
    val tsInMillis = Calendar.getInstance().getTimeInMillis

    def makeTime(i: Int): Long = tsInMillis + (i * 1000) + deltaInMillis

    def makeTimeStr(i: Int): String = new SimpleDateFormat("HH:mm:ss").format(new Date(makeTime(i)))

    def makeMsg(i: Int): String = s"""{"value": "Message ${i * factor + 1}", "ts": "${makeTime(i)}", "timeString": "${makeTimeStr(i)}"}"""

    for (i <- 1 to n) {
      producer.justSend(makeMsg(i + factor))
      println(makeMsg(i))
    }
    /*
    producer.justClose()
    latch.countDown()
    */
    producer
  }

  private def runConsumer(logger: Logger, latch: CountDownLatch): ConsumerThread = {
    logger.info("### Running consumer thread...")
    val GROUP_ID = classOf[ConsumerThread].getSimpleName // +"_"+timeStr
    val BOOTSTRAP_SERVERS = "localhost:9092"

    val props: Properties = new Properties
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    println(s"GroupId config -> ${ConsumerConfig.GROUP_ID_CONFIG}:$GROUP_ID")

    def delay(f: () => Unit, n: Long) = new Timer().schedule(new TimerTask() {
      def run = f()
    }, n)

    val runnable: Runnable = new ConsumerThread(props, logger, latch)
    val consumer: Thread = new Thread(runnable)
    consumer.start() // thread

    runnable.asInstanceOf[ConsumerThread]
  }

  private def runReprocessor(logger: Logger, latch: CountDownLatch, partition: Int, offsetNumber: Long): Reprocessor = {
    val BOOTSTRAP_SERVERS = "localhost:9092"

    val props: Properties = new Properties
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val runnable: Runnable = new Reprocessor(props, logger, latch)
    val reprocessor = runnable.asInstanceOf[Reprocessor]
    logger.info(s"Assign to partition $partition, with offset $offsetNumber")
    reprocessor.setPartitionOffset(partition, offsetNumber)

    new Thread(runnable).start()
    reprocessor
  }
}
