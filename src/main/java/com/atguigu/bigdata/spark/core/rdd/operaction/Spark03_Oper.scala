package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                1,4,3,2
            )
        )

        // TODO collect
        // collect方法用于采集数据
        // 会将Executor执行的结果汇集到Driver的内存中，内存可以会溢出
        // 所以需要谨慎使用
        //val ints: Array[Int] = rdd.collect()

        // TODO count
        val count: Long = rdd.count
        println(count)

        // TODO first
        val first: Int = rdd.first
        println(first)

        // TODO take
        val ints: Array[Int] = rdd.take(3)
        println(ints.mkString(","))

        // TODO takeOrdered
        // 【1，2，3】
        val ints1: Array[Int] = rdd.takeOrdered(3)
        println(ints1.mkString(","))

        sc.stop()
    }
}
