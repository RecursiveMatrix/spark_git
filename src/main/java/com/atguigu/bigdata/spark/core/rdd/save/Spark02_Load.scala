package com.atguigu.bigdata.spark.core.rdd.save

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Load {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("output")
        println(rdd.collect().mkString(","))

        val rdd1: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output1")
        println(rdd1.collect().mkString(","))

        val rdd2: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
        println(rdd2.collect().mkString(","))

        // TODO 关闭环境
        sc.stop()

    }
}
