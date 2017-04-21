package main.scala

import java.util.Properties

import com.github.benfradet.spark.kafka.writer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.sys.process._
import scala.collection.JavaConversions._

//kafka010.writer._
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Created by nsharm9 on 4/3/17.
 */
object SparkKafkaWriter {

   /* This is for performance reason i.e. not to open a multiple connections to Kafka*/
  object PlainKafkaProducerCache {
    @transient private val producers = mutable.HashMap.empty[KafkaProperties, PlainKafkaProducer]

    /**
     * Retrieve a [[PlainKafkaProducer]] in the cache or create a new one
     * @param producerConfig properties for a [[PlainKafkaProducer]]
     * @return a [[PlainKafkaProducer]] already in the cache
     */
    def getProducer(producerConfig: KafkaProperties): PlainKafkaProducer = {
      producers
        .getOrElseUpdate(producerConfig, new PlainKafkaProducer(producerConfig))
        .asInstanceOf[PlainKafkaProducer]
    }
  }

  val producerCache = PlainKafkaProducerCache

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("polaris-connect")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val path = "/hive/hubble_search.db/kafka_events"
    val kafkaProps = new KafkaProperties

    import spark.sql
    import com.github.benfradet.spark.kafka.writer.KafkaWriter

    val query = "select cast(item as string) as item, cast(uniq_page_impr as String) as uniq_page_impr from my_db.search_stats_table where data_date = '2017-03-21' limit 20"

    println("QUERY to execute is :    " + query)
    val q1 = sql(query)


    //Version-1 of publishing the event using Spark UDF

    /*publish message code*/
    val publishMsg = udf((inStr1: String, inStr2: String) => {
      val producerInst = producerCache.getProducer(kafkaProps)
      producerInst.produceRecords(inStr1, inStr2)
      0
    })

    import spark.implicits._
    val distData = q1.select($"item" as "key", $"uniq_page_impr" as "value", publishMsg($"item",$"uniq_page_impr") as "result").cache()
    distData.show()

    distData.write.mode(SaveMode.Overwrite).parquet(path)


    /*
    // This version-2 of the way we do the same thing without UDF
    import spark.implicits._
    val distData = q1.select($"item" as "key", $"uniq_page_impr" as "value").cache()
    distData.show()

    val processedData = distData.rdd.map ( x => {
      val producerInst = producerCache.getProducer(kafkaProps)
      (x.getAs[Int](0), x.getAs[String](1), producerInst.produceRecords(x.getAs[Int](0), x.getAs[String](1)))
    }).toDF("key", "value", "result")

    processedData.write.mode(SaveMode.Overwrite).parquet(path)
    */



    //Version-3 of doing the same thing using third party function (https://github.com/BenFradet/spark-kafka-writer)
    /*
    val distData = {
      import spark.implicits._
      val data = q1.select($"item" as "key", $"uniq_page_impr" as "value")
      data.map(x=>x.getAs[String](0)).rdd
    }
    KafkaWriter.rddToKafkaWriter(distData).writeToKafka(
      kafkaProps.kafkaProducerProp,
      s => new ProducerRecord[String, String](kafkaProps.TOPIC, s)
    )
    */

  }
}
