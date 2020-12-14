package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming02_DStream_Dir {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 连接环境
        // 伴生对象的apply方法用于创建当前伴生类的对象
        // 伴生对象的apply方法其实只是用于构建对象，不一定非得创建伴生类对象
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val dirDS: DStream[String] = ssc.textFileStream("in")

        val wordDS: DStream[String] = dirDS.flatMap(line => line.split(" "))

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
