package com.telekosmos.kafka.course

import java.text.SimpleDateFormat
import java.util.concurrent.CountDownLatch
import java.util._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

object Main extends App {
  private def dateStr(d: Date): String = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss").format(d)
  // (new ProducerDemo).justSend(s"""{"value": "Yet another message", "ts": "$dateStr"}""")
  private def timeStr(d: Date): String = new SimpleDateFormat("HH:mm:ss").format(d)

  override def main(args: Array[String]) = {
    var latch: CountDownLatch = new CountDownLatch(1)
    val logger: Logger = LoggerFactory.getLogger(classOf[ConsumerThread])
    val consumerThread: ConsumerThread = runConsumer(logger, latch)
    // runProducer(20, latch)
    sys.addShutdownHook({
      println("@@@ Entering shutdownhook")
      // consumerThread.interrupt()
      // runnable.asInstanceOf[ConsumerThread].shutdown()
      consumerThread.shutdown()
      try {
        latch.await()
      }
      catch {
        case i:InterruptedException => logger.info("@@@ App was interrupted [shutdownhook acting]")
      }
      finally {
        println("@@@ FINALLY")
      }
    })

    try {
      logger.info("@@@ Awaiting for latch")
      latch.await()
    } catch {
      case e:InterruptedException => println(s"Application got interrupted: $e")
    } finally {
      println("Application is closing")
    }
  }

  def runProducer(n:Int, latch: CountDownLatch): Unit = {
    val producer = new ProducerDemo
    val deltaInMillis:Int = 2500;
    val tsInMillis = Calendar.getInstance().getTimeInMillis

    def makeTime(i:Int): Long = tsInMillis+(i*1000)+deltaInMillis
    def makeTimeStr(i:Int): String = new SimpleDateFormat("HH:mm:ss").format(new Date(makeTime(i)))
    def makeMsg(i:Int): String = s"""{"value": "Message $i", "ts": "${makeTime(i)}", "timeString": "${makeTimeStr(i)}"}"""
    for (i <- 1 to n) {
      producer.justSend(makeMsg(i))
      println(makeMsg(i))
    }
    producer.justClose()
    latch.countDown()
  }

  private def runConsumer(logger: Logger, latch: CountDownLatch): ConsumerThread = {
    val GROUP_ID = classOf[ConsumerThread].getSimpleName // +"_"+timeStr
    val BOOTSTRAP_SERVERS = "localhost:9092"

    val props: Properties = new Properties
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    println(s"GroupId config -> ${ConsumerConfig.GROUP_ID_CONFIG}:$GROUP_ID")

    // var latch: CountDownLatch = new CountDownLatch(1)
    def delay(f: () => Unit, n: Long) = new Timer().schedule(new TimerTask() {
      def run = f()
    }, n)

    val runnable: Runnable = new ConsumerThread(props, logger, latch)
    val consumer: Thread = new Thread(runnable)
    consumer.start() // thread
/*
    val interruptRunnable = new Interrupt(latch)
    val interruptThread = new Thread(interruptRunnable)
    interruptThread.start()
*/
    /*sys.addShutdownHook({
      println("@@@ Entering shutdownhook")
      interruptThread.interrupt()
      // runnable.asInstanceOf[ConsumerThread].shutdown()
      // consumer.close()
      try {
        latch.await()
      }
      catch {
        case i:InterruptedException => println("@@@ App was interrupted [shutdownhook acting]")
      }
      finally {
        println("@@@ FINALLY")
      }
    })
    // thread.start()

    try {
      System.out.println("@@@ Awaiting for latch")
      latch.await()
    } catch {
      case e:InterruptedException => println(s"Application got interrupted: $e")
    } finally {
      println("Application is closing")
    }*/

    println("XXXXX End DEMO")
    runnable.asInstanceOf[ConsumerThread]
  }
  // KafkaExample.run()
}
