package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Memory_Part_2 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // 0， 1， 2， 3
        // 0 => 1 * 6 / 4 => 【0, 1】 => 1
        // 1 => 2 * 6 / 4 => 【1, 3】 => 2, 3
        // 2 => 3 * 6 / 4 => 【3, 4】 => 4
        // 3 => 4 * 6 / 4 => 【4, 6】 => 5, 6
        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 4)
        rdd.saveAsTextFile("output")


        sc.stop()
    }
}
