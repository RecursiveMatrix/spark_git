package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Memory_Part {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        sparkConf.set("spark.default.parallelism", "5")
        val sc : SparkContext = new SparkContext(sparkConf)

        // RDD的分区
        // 1. makeRDD的第二个参数表示分区数量，如果设定，那么Spark会按照这个数值进行分区
        // val rdd = sc.makeRDD(List(1,2,3,4),3)
        // makeRDD的第二个参数表示分区数量，如果没有设定
        // scheduler.conf.getInt("spark.default.parallelism", totalCores)
        // 2. 从配置对象中获取指定的配置参数
        // 3. 获取当前环境中总共的核数
        val rdd = sc.makeRDD(List(1,2,3,4))
        // 将数据保存到分区文件
        rdd.saveAsTextFile("output")


        sc.stop()
    }
}
