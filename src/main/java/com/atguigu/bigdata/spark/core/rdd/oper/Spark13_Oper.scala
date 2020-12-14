package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(1,2,3,4), 2)
        val rdd2 = sc.makeRDD(List(3,4,5,6), 2)

        val rdd3 = rdd1.intersection(rdd2)
        println("交集 = " + rdd3.collect().mkString(",")) // 3，4

        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println("并集 = " + rdd4.collect().mkString(",")) // 1，2，3，4，3，4，5，6

        val rdd5 = rdd1.subtract(rdd2)
        println("差集 = " + rdd5.collect().mkString(",")) // 1，2

        val rdd6 = sc.makeRDD(List("3", "4", "5", "6"), 2)
        // 交集,并集，差集操作时，需要两个RDD的数据类型保持一致
        //rdd1.intersection(rdd6) (X)
        //rdd1.union(rdd6) (X)
        //rdd1.subtract(rdd6)

        sc.stop()
    }
}
