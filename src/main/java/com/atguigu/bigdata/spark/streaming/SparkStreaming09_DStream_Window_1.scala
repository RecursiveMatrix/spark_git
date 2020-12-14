package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_DStream_Window_1 {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")
        val ds = ssc.socketTextStream("localhost", 7777)

        // 无状态操作
        val wordDS = ds.flatMap(_.split(" "))
        val wordToOneDS = wordDS.map((_,1))

        // The checkpoint directory has not been set
        // TODO：reduceByKeyAndWindow方法一般在窗口范围大，但是滑动步长小的时候使用
        // 因为存在大量的重复数据的计算，会影响性能
        // reduceByKeyAndWindow需要保存中间处理状态
        val windowDS = wordToOneDS.reduceByKeyAndWindow(
            (x, y) => {
                println(x + "+" + y)
                x + y
            },
            (a, b) => {
                println(a + "-" + b)
                a - b
            },
            Seconds(9),
            Seconds(3)
        )

        windowDS.print()


        ssc.start()
        ssc.awaitTermination()
    }
}
