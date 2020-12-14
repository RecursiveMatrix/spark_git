package com.atguigu.bigdata.spark.streaming

import java.util.Random

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming12_Req_Mock {

    def main(args: Array[String]): Unit = {

        // 向Kafka中发送模拟数据
        /**
          * 模拟的数据
          *
          * 格式 ：timestamp area city userid adid
          * 某个时间点 某个地区 某个城市 某个用户 某个广告
          */
        val topic = "atguigu0820"

        // 创建配置对象
        val prop = new java.util.Properties()
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux1:9092")
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](prop)

        while ( true ) {
            // 向Kafka发送的模拟数据对象
            for ( data <- mockData() ) {
                val record = new ProducerRecord[String, String](topic, data)
                producer.send(record)
                println(data)
            }

            Thread.sleep(2000)
        }
    }
    def mockData() = {
        val areas = List("华北", "华南", "华中")
        val cities = List("北京", "上海", "深圳")
        val list = ListBuffer[String]()
        for ( i <- 1 to new Random().nextInt(50) ) {
            val area = areas(new Random().nextInt(3))
            val city = cities(new Random().nextInt(3))
            val userid = new Random().nextInt(6) + 1
            val adid = new Random().nextInt(6) + 1

            val dataString = s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}"
            list.append(dataString)
        }
        list
    }
}
