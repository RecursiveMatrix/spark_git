package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,3,2,5),2)

        val maxRDD = rdd.mapPartitions(
            iter => {
                List(iter.max).iterator // Iterable
            }
        )

        maxRDD.collect().foreach(println)

        sc.stop()
    }
}
