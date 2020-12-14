package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Oper_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)
        // 【1，2】，【3，4】，【5，6】
        // 【2】，【4】，【6】
        // 12
//        val rdd1: RDD[Int] = rdd.mapPartitions(
//            iter => {
//                List(iter.max).iterator
//            }
//        )
//        println(rdd1.sum())
        val rdd1:RDD[Array[Int]] = rdd.glom()
        val rdd2: RDD[Int] = rdd1.map(array=>array.max)
        val ints: Array[Int] = rdd2.collect()
        println(ints.sum)



        sc.stop()
    }
}
