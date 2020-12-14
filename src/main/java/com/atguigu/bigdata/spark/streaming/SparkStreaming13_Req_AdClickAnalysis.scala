package com.atguigu.bigdata.spark.streaming

import java.sql.ResultSet
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming13_Req_AdClickAnalysis {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

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

        // TODO 需求二 ：广告点击量实时统计

        val reduceDS = kafkaVal.map(
            line => {
                val datas = line.split(" ")
                // 每天各地区各城市各广告的点击
                // 归一
                //
                // (( 天，地区，城市， 广告 ), cnt)
                val sdf = new SimpleDateFormat("yyyy-MM-dd")

                val day = sdf.format(new java.util.Date(datas(0).toLong))
                val area = datas(1)
                val city = datas(2)
                val ad = datas(4)
                ( (day, area, city, ad), 1 )
            }
        ).reduceByKey(_+_)

        reduceDS.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        val conn = JdbcUtil.getConnection
                        iter.foreach {
                            case ( (day, area, city, ad), sum ) => {
                                println(((day, area, city, ad), sum))
                                val sql =
                                    """
                                      | insert into area_city_ad_count (dt, area, city, adid, count) values ( ?, ?, ?, ?, ? )
                                      | ON DUPLICATE KEY
                                      | UPDATE count = count + ?
                                    """.stripMargin

                                JdbcUtil.executeUpdate(conn, sql , Array(day, area, city, ad, sum, sum))
                            }
                        }

                        conn.close()
                    }
                )
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }

}
