package com.atguigu.bigdata.spark.streaming

import java.util.Random

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_DStream_Kafka {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 从Kafka中消费数据，用于数据分析
        // 由于在实际工作中，实时数据处理基本上都是采用kafka完成的
        // 所以为了开发方便，kafka提供了工具类完成基本的数据操作
        val kafkaPara: Map[String, Object] =
            Map[String, Object](
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
                ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
                "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
            )

        // Kafka中的数据以 k-v对 进行传递
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](Set("atguigu0820"), kafkaPara))

        val kafkaVal: DStream[String] = kafkaDStream.map(_.value())

        kafkaVal.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
