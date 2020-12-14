package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming10_DStream_Output {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")
        val ds = ssc.socketTextStream("localhost", 7777)

        // 无状态操作
        val wordDS = ds.flatMap(_.split(" "))
        val wordToOneDS = wordDS.map((_,1))
        val windowDS = wordToOneDS.reduceByKey(_+_)

        //windowDS.print()
        // DStream的输出操作不是很多，所以想要进行特殊的输出操作，必须获取底层的RDD进行处理
        // DStream transform方法和foreachRDD方法的区别就在于RDD是否返回
        // 类似于RDD 的map方法和foreach方法
        windowDS.transform(
            rdd => {
                rdd
            }
        )
        windowDS.foreachRDD(
            rdd => {
                // Coding : 周期性执行代码
                // JDBC
                // Jedis
                // Hbase
            }
        )


        ssc.start()
        ssc.awaitTermination()
    }
}
