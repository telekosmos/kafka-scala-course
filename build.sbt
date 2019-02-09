name := "scaladev"

version := "0.1"

scalaVersion := "2.12.5"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
// if Test for log4j, logs are flooded with DEBUG traces :-S
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.25" % Compile
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.1.0"
