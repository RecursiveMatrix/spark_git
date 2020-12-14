package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(1,2,3,4)) // ParallelCollectionRDD
        // Spark的RDD对象中包含了很多的方法（算子）
        // 这些方法根据功能的不同分为2大类
        // TODO 1. 转换（transform）,通过调用方法，将旧的RDD转换为新的RDD
        val newRDD: RDD[Int] = rdd.map(_*2) // MapPartitionsRDD

        // TODO 2. 行动（action），通过调用方法，将RDD开始执行
        newRDD.collect()

        sc.stop()
    }
}
