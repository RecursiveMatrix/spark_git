package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Oper_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)

        // TODO - 缩减分区

        // coalesce算子的第一个参数表示缩减分区的数量，但是这个值可以比原分区大
        // 但是如果不shuffle的情况下，不起作用
        val rdd1 = rdd.coalesce(6, true)

        rdd1.saveAsTextFile("output")


        sc.stop()
    }
}
