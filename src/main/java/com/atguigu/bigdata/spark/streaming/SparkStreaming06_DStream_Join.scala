package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_DStream_Join {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val ds1 = ssc.socketTextStream("localhost", 9999)
        val ds2 = ssc.socketTextStream("localhost", 8888)

        val kv1: DStream[(String, Int)] = ds1.map((_,1))
        val kv2: DStream[(String, Int)] = ds2.map((_,1))

        val joinDS: DStream[(String, (Int, Int))] = kv1.join(kv2)

        joinDS.print()



        ssc.start()
        ssc.awaitTermination()
    }
}
