# kafka-spark-processor

Welcome to the kafka-spark-processor wiki!

Please look at the sample code that i wrote about how to produce events using spark job. 
The main motivation to write this code came from our data science use cases where we run all our DS alogs on huge amount of data and then the finals results of that algos goes to the search indexes (publish results to the kafka events which will then update the search indexes ). One way to deal with this use case is to train the models separately as a separate job and write a different job to read from HDFS to publish events to kafka ( either using kafka-hdfs connector ( kafka to hdfs is available but not sure hdfs to kafka is available or not ) or parallelize your kafka producer using containers ), which finally updates the indexes. But that is quite un-optimized way as we need to maintain two different code base and jobs. So i wrote this job to process the data, run algos and also publish the final results to update the search indexes in just one simple job using spark dataframes. In other words, this is the replica of spark streaming job which you can run anytime ( on demand ) rather than streaming.

Just to be clear that this is not using Spark streaming job. This is an example when you want to use your batch Spark job to process huge amount of data in batches which cannot be done using any streaming pipeline and then finally publishing the output as key, value pairs for people to consume in a streaming fashion. Also, this is an example on how to use Spark dataframes effectively to process and publish data at the same time.

SparkKafkaWriter is the scala class which has all the three version to push events to kafka.
Version 1:  Using Spark dataframes UDF
Version 2:  Using Spark mapper
Version 3:  Using 3rd party Spark-Kafka producer.

Also, PlainKafkaProducer has an example for both standalone kafka producer and also Spark kafka producer and the same is there for kafka consumer in PlainKafkaConsumer.
Also, i have added the code for partitioner in PlainKafkaPartitioner which can be used in case you want to partition your events and publish them in a specific kafka partitions.

I hope this helps!!

