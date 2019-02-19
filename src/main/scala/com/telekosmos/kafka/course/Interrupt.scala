package com.telekosmos.kafka.course

import java.util.concurrent.CountDownLatch

// object Interrupt  { extends App{
class Interrupt(latch: CountDownLatch) extends Runnable {

  // override
  def main(args: Array[String]): Unit = {
    run()
  }

  override def run(): Unit = {
    println("Running runnable Interrupt...")
    var counter = 1
    while (!Thread.currentThread.isInterrupted) {
      try {
        counter += 1
        print(s"\r$counter")
      }
      catch {
        case e: InterruptedException => {
          println(s"Interrupted Exception: $e")
          Thread.currentThread.interrupt()
        }
        case et:Throwable => {
          println(s"Another exception: $et")
          throw et
        }
      }
    } // EO while
    println("Shutdown resources properly... latch.countdown")
    latch.countDown()
  }

  def runThread(): Unit = {
    val zmqThread = new Thread(new Runnable {
      def run() {
        println("Running Interrupt...")
        var counter = 1
        while (!Thread.currentThread.isInterrupted) {
          try {
            counter += 1
            print(s"\r$counter")
          } catch {
            case e: InterruptedException => {
              println(s"Interrupted Exception: $e")
              Thread.currentThread.interrupt()
            }
            case et:Throwable => {
              println(s"Another exception: $et")
              throw et
            }
          }
        }
        println("Resources shutdown complete")
      }
      /*
      sys.addShutdownHook({
        println("INSIDE THE THREAD: ShutdownHook called")
        // Thread.currentThread.interrupt()
      })
      */
    })
    /*
    sys.addShutdownHook({
      println("ShutdownHook called")
      zmqThread.interrupt()
      zmqThread.join
    })
    */
    zmqThread.start()
  }
}