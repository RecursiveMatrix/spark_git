package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Oper_2 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)

        // TODO - 缩减分区
        val rdd1 = rdd.coalesce(2, true)
        // TODO - 扩大分区
        // repartition底层调用的其实就是coalesce方法，只不过一定会有shuffle操作
        val rdd2 = rdd.repartition(6)


        rdd1.saveAsTextFile("output")


        sc.stop()
    }
}
