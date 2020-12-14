package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming11_DStream_Close {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming close
        // 关闭
        // 在采集业务发生变化或技术升级时，需要将采集功能进行关闭的。
        // 关闭时需要调用环境对象stop方法
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        val ds = ssc.socketTextStream("localhost", 7777)

        // 无状态操作
        val wordDS = ds.flatMap(_.split(" "))
        val wordToOneDS = wordDS.map((_,1))
        val windowDS = wordToOneDS.reduceByKey(_+_)
        windowDS.print()

        ssc.start()

        // 通过简单的了解后，stop方法不能在主线程中调用的。
        // 1. 关闭需要在新的线程中执行
        // 2. 方法的调用时机 (分布式事务)
        //    JDBC => Table => closeflg(0) => 1
        //    ZK => node => /stopSpark
        //    HDFS => path => /stopSpark
        //    Redis => data => stopSpark
        //var closeFlg = false

        // 3. 优雅地关闭

        new Thread(new Runnable {
            override def run(): Unit = {
//                while ( true ) {
//                    if ( closeFlg ) {
//                        ssc.stop()
//                        return
//                    }

                    //Thread.sleep(10000)
               // }

                Thread.sleep(5000)

                // 关闭SparkStreaming操作
                ssc.stop(true, true)
                System.exit(0)

            }
        }).start()

        ssc.awaitTermination()
        //ssc.stop()
    }
}
