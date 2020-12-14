package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                1,2,3,4
            ),
            3
        )
        // 10 + 1 + 2 => 13
        // 10 + 3 + 4 => 17
        // 10 + 13 + 17 => 40 + 10

        // TODO aggregate
        // aggregateByKey算子的初始值用于分区内计算
        // aggregate算子的初始值分区间也会使用
        val i: Int = rdd.aggregate(10)(_+_, _+_)
        println(i)

        // TODO fold
        val i1: Int = rdd.fold(10)(_+_)
        println(i1)

        sc.stop()
    }
}
