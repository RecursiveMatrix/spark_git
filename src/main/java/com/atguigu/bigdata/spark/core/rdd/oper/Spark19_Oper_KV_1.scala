package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Oper_KV_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO :将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值

        // 【("a", 88), ("b", 95), ("a", 91)】
        // 【("b", 93), ("a", 95), ("b", 98)】
        val rdd = sc.makeRDD(
            List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),
            2
        )


        // TODO combineByKey可以实现wordcount, 6/10
        val rdd1: RDD[(String, Int)] = rdd.combineByKey(
            (num: Int) => num,
            (t: Int, num: Int) => {
                t + num
            },
            (t1: Int, t2: Int) => {
                t1 + t2
            },
        )

        rdd1.collect().foreach(println)






        sc.stop()
    }
}
