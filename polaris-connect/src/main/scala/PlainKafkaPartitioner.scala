package main.scala

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
 * Created by nsharm9 on 4/4/17.
 */
class PlainKafkaPartitioner extends Partitioner{
  override def close(): Unit = ???

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = key.hashCode() % (cluster.availablePartitionsForTopic("polaris-signal-test").size() )

  override def configure(configs: util.Map[String, _]): Unit = {

    println("Inside Partitioner configure " + configs)

  }
}
