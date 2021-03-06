package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_DStream_Queue {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 连接环境
        // 伴生对象的apply方法用于创建当前伴生类的对象
        // 伴生对象的apply方法其实只是用于构建对象，不一定非得创建伴生类对象
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val rddQueue = new mutable.Queue[RDD[Int]]()

        val inputStream = ssc.queueStream(rddQueue)
        val mappedStream = inputStream.map((_,1))
        val reducedStream = mappedStream.reduceByKey(_ + _)

        reducedStream.print()

        ssc.start()

        for (i <- 1 to 5) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()

        // 关闭环境
        //ssc.stop()

    }
}
