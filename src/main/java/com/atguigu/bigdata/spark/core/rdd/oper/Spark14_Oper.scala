package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(1,2,3,4), 2)
        val rdd2 = sc.makeRDD(List(3,4,5,6), 2)
        val rdd4 = sc.makeRDD(List("7","8","9","a"), 2)

        // SparkException : Can only zip RDDs with same number of elements in each partition
        // Can't zip RDDs with unequal numbers of partitions: List(3, 2)
        // spark RDD的zip操作要求分区数量保持一致，并且每个分区的元素数量保持一致
        val rdd3 = rdd1.zip(rdd2)
        println(rdd3.collect().mkString(","))

        // 两个数据源的类型不相同，也可以拉链
        val value: RDD[(Int, String)] = rdd2.zip(rdd4)


        sc.stop()
    }
}
