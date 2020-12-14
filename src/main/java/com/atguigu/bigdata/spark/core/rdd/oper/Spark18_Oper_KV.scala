package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_Oper_KV {

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
        // 【("a", 1), ("b", 2), ("a", 3)】
        // 【("b", 4), ("b", 5), ("a", 6)】
        // =>
        // 【("b", 2), ("a", 3)】
        // 【("b", 5), ("a", 6)】
        // =>
        // 【("a", 6), ("a", 3)】 => 【(a, 9)】
        // 【("b", 5), ("b", 2)】 => 【(b, 7)】
        // aggregateByKey有2个参数列表
        // 第一个参数列表有一个参数，表示计算的初始值
        //       用于分区内相同key的第一个value的计算
        // 第二个参数列表有二个参数
        //      第一个参数表示分区内计算功能（函数）
        //      第二个参数表示分区间计算功能（函数）
        val rdd1 = rdd.aggregateByKey(5)(
            (x : Int, y : Int) => {
                math.max(x, y)
            },
            (a : Int, b : Int) => {
                a + b
            }
        )

        rdd1.collect().foreach(println)





        sc.stop()
    }
}
