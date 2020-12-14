package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd : RDD[Int] = sc.makeRDD(
            List(
               1,2,3,4
            ),2
        )
        // 【Array(1,2), Array(3,4)】
        val rdd1: RDD[Array[Int]] = rdd.glom()

        rdd1.collect().foreach(array=>println(array.mkString(",")))



        sc.stop()
    }
}
