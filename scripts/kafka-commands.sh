kafka-topics.sh --zookeeper zookeeper:2181 --describe --topic scala-topic
kafka-topics.sh --zookeeper zookeeper:2181 --create --topic scala-topic --partitions 3 --replication-factor 1
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic scala-topic --group kafka-4-scala
