package com.atguigu.bigdata.spark.streaming

import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming03_DStream_DIY {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 创建自定义数据采集器
        val receiver = new MyReceiver
        // 应用自定义采集器
        val inputStream = ssc.receiverStream(receiver)
        val mappedStream = inputStream.map((_,1))
        val reducedStream = mappedStream.reduceByKey(_ + _)

        reducedStream.print()

        ssc.start()


        ssc.awaitTermination()
    }
    // 自定义数据采集对象
    // 1. 继承Receiver类, 定义泛型
    // 2. 给父类传递初始化参数，用于设定数据的存储级别
    // 3. 重写方法
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
        private var flowFlg = true
        override def onStart(): Unit = {
            while ( flowFlg ) {
                Thread.sleep(100)
                // 生成数据
                val data = new Random().nextInt(100).toString
                // 存储数据
                store(data)
            }
        }

        override def onStop(): Unit = {
            flowFlg = false
        }
    }
}
