package main.scala

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._

/**
 * Created by nsharm9 on 4/4/17.
 */

/*Stand alone kafka producer implementation */

object PlainKafkaConsumer extends App{


  val kafkaProperties = new KafkaProperties
  val  props = kafkaProperties.kafkaConsumerProp()
  val t = System.currentTimeMillis()

  println("I am start consuming the topic")
  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(kafkaProperties.TOPIC))

  while(true){
    val records=consumer.poll(100)
    for (record<-records.asScala){
      println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset() + " partition:" + record.partition())
    }
  }
}
