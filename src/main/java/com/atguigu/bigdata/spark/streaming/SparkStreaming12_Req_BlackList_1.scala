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

object SparkStreaming12_Req_BlackList_1 {

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

        // 从Kafka中获取数据 line
        // 分解从kafka中获取的数据

        val reduceDS = kafkaVal.transform(
            rdd => {
                // TODO 周期性获取黑名单数据
                val blackList = ListBuffer[String]()

                val conn = JdbcUtil.getConnection
                val pstat = conn.prepareStatement("select userid from black_list")
                val rs = pstat.executeQuery()
                while ( rs.next() ) {
                    blackList.append(rs.getString(1))
                }
                rs.close()
                pstat.close()
                conn.close()

                // TODO 判断用户是否在黑名单中(过滤)
                val restRDD = rdd.filter(
                    line => {
                        val datas = line.split(" ")
                        val userid = datas(3)
                        !blackList.contains(userid)
                    }
                )

                // TODO 对用户的广告点击进行采集周期内的统计
                // （（天，用户，广告），1）
                val reduceRDD: RDD[((String, String, String), Int)] = restRDD.map(
                    line => {
                        val datas = line.split(" ")
                        // 毫秒 => 天
                        // SimpleDateFormat
                        val sdf = new SimpleDateFormat("yyyy-MM-dd")

                        val day = sdf.format(new java.util.Date(datas(0).toLong))
                        val user = datas(3)
                        val ad = datas(4)

                        ((day, user, ad), 1)
                    }
                ).reduceByKey(_ + _)

                reduceRDD
            }
        )
        reduceDS.print()
        //  Task not serializable
        // 所有的连接对象都没有办法序列化
        reduceDS.foreachRDD(
            rdd => {
                val conn = JdbcUtil.getConnection
                rdd.foreach {
                    // ((天，用户，广告1), 150)
                    // ((天，用户，广告2), 150)
                    case ((day, user, ad), sum) => {
                        // TODO 判断统计结果是否超过阈值
                        if ( sum >= 20 ) {
                            // 超过阈值的场合
                            // TODO 将当前用户插入到黑名单的表中

                            // SQL注入
                            val sql =
                                """
                                  | insert into black_list(userid) values (?)
                                  | ON DUPLICATE KEY
                                  | UPDATE userid=?
                                """.stripMargin
                            JdbcUtil.executeUpdate(conn, sql, Array(user, user))
                        } else {
                            // 未超过阈值的场合
                            // TODO 获取用户统计信息
                            val pstat = conn.prepareStatement(
                                """
                                  | select
                                  |    count
                                  | from user_ad_count
                                  | where dt = ? and userid = ? and adid = ?
                                """.stripMargin)
                            pstat.setString(1, day)
                            pstat.setString(2, user)
                            pstat.setString(3, ad)
                            val rs: ResultSet = pstat.executeQuery()
                            if ( rs.next() ) {
                                // TODO 存在数据的场合
                                // 聚合当前数据和统计数据，判断是否超过阈值
                                val count: Int = rs.getInt(1)
                                if ( count + sum >= 20 ) {
                                    // TODO 如果超过阈值，将用户拉入到黑名单中
                                    // SQL注入
                                    val sql =
                                    """
                                      | insert into black_list(userid) values (?)
                                      | ON DUPLICATE KEY
                                      | UPDATE userid=?
                                    """.stripMargin
                                    JdbcUtil.executeUpdate(conn, sql, Array(user, user))
                                } else {
                                    // TODO 如果没有超过阈值，更新统计信息
                                    val sql = """
                                          | update user_ad_count
                                          | set count = ?
                                          | where dt = ? and userid = ? and adid = ?
                                        """
                                    JdbcUtil.executeUpdate(conn, sql, Array(count + sum, day, user, ad))
                                }
                            } else {
                                // TODO 没有数据的场合, 直接插入统计信息
                                val sql = """
                                      | insert into user_ad_count (dt, userid, adid, count) values ( ?, ?, ?, ? )
                                    """
                                JdbcUtil.executeUpdate(conn, sql, Array(day, user, ad, sum))
                            }
                            rs.close()
                            pstat.close()
                        }
                    }
                }
                conn.close()
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }

}
