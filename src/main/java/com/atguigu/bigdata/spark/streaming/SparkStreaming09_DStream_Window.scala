package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_DStream_Window {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val ds = ssc.socketTextStream("localhost", 7777)

        // 无状态操作
        val wordDS = ds.flatMap(_.split(" "))
        val wordToOneDS = wordDS.map((_,1))

        // 计算前，可以设定数据的操作范围（窗口）
        // 窗口window方法用于进行数据的窗口计算
        // 默认情况下，窗口随着时间推移进行滑动的，默认值以一个采集周期
        // SparkStreaming中计算周期为窗口滑动步长
        // 窗口操作默认就是存在，只不过默认范围为一个采集周期，滑动幅度为一个采集周期
        // 默认的窗口操作不需要状态保存，只需要默认处理即可。
        // 其中会传递2个参数
        //   第一个参数表示窗口的范围，以时间为单位，一般设定为采集周期的整数倍
        //   第二个参数表示窗口的滑动幅度，步长，一般设定为欸采集周期的整数倍
        val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(6), Seconds(6))

        val wordCountDS = windowDS.reduceByKey(_+_)

        wordCountDS.print()


        ssc.start()
        ssc.awaitTermination()
    }
}
