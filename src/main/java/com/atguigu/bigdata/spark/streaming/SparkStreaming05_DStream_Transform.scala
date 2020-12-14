package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_DStream_Transform {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val ds = ssc.socketTextStream("localhost", 9999)

        //ds.map(s=>s)
        // DStream底层封装了RDD，但是提供的方法并不完整，所以如果想要实现特定的功能，如排序
        // 那么需要将DStream转换成RDD进行操作
        //ds.transform(rdd=>rdd.sortBy(1))

        //ds.map(_*2)
        //ds.transform(rdd=>rdd.map(_*2))

        // TODO 原语，算子，方法
        // Coding => Driver端 (执行一次)
        ds.map(
            str => {
                // Coding => Executor端(任务数量)
                str * 2
            }
        )

        // ===========================
        // Coding => Driver端 (执行一次)
        ds.transform(
            rdd => {
                // Coding => Driver端 (周期性执行)
                rdd.map(
                    str => {
                        // Coding => Executor端 (任务数量)
                        str * 2
                    }
                )
            }
        )


        ssc.start()
        ssc.awaitTermination()
    }
}
