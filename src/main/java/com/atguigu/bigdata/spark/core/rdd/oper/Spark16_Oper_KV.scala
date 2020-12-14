package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark16_Oper_KV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                ("a", 1), ("b", 1), ("a", 1), ("b", 1)
            ),2
        )

        // TODO reduceByKey将相同key的数据汇总在一起后，对v进行聚合操作
        // reduceByKey也能实现wordcount，2/10
        // ("a", 1),("a", 1)

        // reduceByKey在分区内预聚合和分区间聚合时，数据处理的逻辑是相同的。
        val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_+_)

        // word => count
        println(rdd1.collect().mkString(","))


        sc.stop()
    }
}
