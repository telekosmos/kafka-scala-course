package com.telekosmos.kafka.course.util

import scala.util.Try

object Utils {
  private def getParamValue(args: Array[String], paramName:String): String = {
    val argPos = args.indexOf(paramName)
    if (argPos == -1) "" else args(argPos+1)
  }

  def tryToInt(s:String): Option[Int] = Try(s.toInt).toOption

  def getNumOfProducers(args: Array[String]): Option[Int] = tryToInt(getParamValue(args, "-p"))

  def getNumOfConsumers(args: Array[String]): Option[Int] = tryToInt(getParamValue(args,"-c"))
}
