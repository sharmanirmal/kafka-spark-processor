package main.scala

import java.util.Date
import scala.sys.process._
import scala.collection.JavaConversions._

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._

/**
 * Created by nsharm9 on 4/4/17.
 */

/*Stand alone kafka producer implementation */
/*
object PlainKafkaProducer extends App{


  val kafkaProperties = new KafkaProperties
  val  props = kafkaProperties.kafkaProducerProp()
  val t = System.currentTimeMillis()

  val producer = new KafkaProducer[Int, String](props)

  for(i<- 1 to 10){
    val runtime = new Date().getTime()
    val record = new ProducerRecord[Int, String](kafkaProperties.TOPIC, i, s"hello now $i")
    producer.send(record)
  }

  val record = new ProducerRecord[Int, String](kafkaProperties.TOPIC, 100, "the end " + new java.util.Date)
  producer.send(record)

  producer.close()

}*/

/*Spark based kafka producer implementation */
case class PlainKafkaProducer(key: String, value : String){


  def produceRecords : Int = {

    val kafkaProperties = new KafkaProperties
    val  props = kafkaProperties.kafkaProducerProp()
    val t = System.currentTimeMillis()

    val producer = new KafkaProducer[String, String](props)

    //println("Key: " + key + " Value: " + value)
    val record = new ProducerRecord[String, String](kafkaProperties.TOPIC, key, value)
    producer.send(record)


    producer.close()

    0

  }


}
