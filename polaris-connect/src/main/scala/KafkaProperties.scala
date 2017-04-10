package main.scala

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * Created by nsharm9 on 4/4/17.
 */
class KafkaProperties extends Serializable{

  val TOPIC = "polaris-signal-test"
  val BROKERS = "my.borker.server:9092" //Give the list of all the brokers
  val PRODUCER = "SparkKafkaProducer"

  val CONSUMER = "SparkKafkaConsumer"
  val GROUPID = "group-A"

  def kafkaProducerProp(): Properties = {

      val props = new Properties()
      props.put("bootstrap.servers", BROKERS)
      props.put("client.id", PRODUCER)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("partitioner.class", "main.scala.PlainKafkaPartitioner") //Exclude this one if you dont require partitioner
      props.put("acks", "all")
      props

  }

  def kafkaConsumerProp(): Properties = {

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS)
    //props.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-A")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props

  }
}

