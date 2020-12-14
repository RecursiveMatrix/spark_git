package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 连接环境
        // 伴生对象的apply方法用于创建当前伴生类的对象
        // 伴生对象的apply方法其实只是用于构建对象，不一定非得创建伴生类对象
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 从指定端口中获取输入的数据
        // 获取的数据是一行一行
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val wordDS: DStream[String] = socketDS.flatMap(line => line.split(" "))

        val wordToOneDS = wordDS.map((_,1))

        val reduceDS = wordToOneDS.reduceByKey(_+_)

        reduceDS.print()
        //println(reduceDS)

        // 启动采集器
        ssc.start()
        // 阻塞当前运行的线程。Driver不能执行完毕，需要等待采集器的结束
        ssc.awaitTermination()

        // 关闭环境
        //ssc.stop()

    }
}
