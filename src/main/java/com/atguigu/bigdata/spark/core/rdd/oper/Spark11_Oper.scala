package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)

        // TODO - 缩减分区
        // coalesce算子用于将多分区缩减为少分区，默认情况下不会将数据打乱重新组合，没有shuffle
        // 默认合并规则采用的是计算位置的关系
        // 如果想要让合并分区后的数据更均衡一些的话，那么可以使用shuffle
        // coalesce的第二个参数表示是否使用shuffle,默认值为false，不使用shuffle
        val rdd1 = rdd.coalesce(2, true)

        rdd1.saveAsTextFile("output")


        sc.stop()
    }
}
