package com.atguigu.bigdata.spark.streaming

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object SparkStreaming14_Req_LastHourAdClick {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

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

        // TODO 需求三 ：最近一小时某个广告点击量趋势统计
        //     ( 1分钟， 每10秒计算 )
        //   word, cnt(1)
        val timeDS = kafkaVal.map(
            line => {
                // 25 => 1
                // 26 => 20
                // 31 => 30
                // 59 => 50
                //val i = 51
                //i / 10 * 10 = 50

                val datas = line.split(" ")
                val ts = datas(0).toLong
                val stime = ts / 10000 * 10000
                (stime, 1)
            }
        )

        val reduceDS = timeDS.reduceByKeyAndWindow(
            (x:Int, y:Int) => x + y,
            Minutes(1),
            Seconds(10)
        )

        reduceDS.print


        ssc.start()
        ssc.awaitTermination()
    }

}
