package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_Oper_KV_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO 取出每个分区内相同key的最大值然后分区间相加
        val rdd = sc.makeRDD(
            List(
                ("a", 1), ("b", 2), ("b", 3),
                ("b", 4), ("b", 5), ("a", 6),
            ),2
        )

        // aggregateByKey算子的第二个参数列表的两个参数可以一致的
        // 分区内计算规则和分区间计算规则可以相同
        // TODO aggregateByKey可以实现wordcount  4/10
        //val rdd1 = rdd.aggregateByKey(0)(_+_, _+_)

        // 如果aggregateByKey的分区内计算规则和分区间计算规则相同的话
        // 那么可以简化为另外一个方法foldByKey
        // TODO foldByKey可以实现wordcount  5/10
        val rdd2 = rdd.foldByKey(0)(_+_)

        rdd2.collect().foreach(println)





        sc.stop()
    }
}
