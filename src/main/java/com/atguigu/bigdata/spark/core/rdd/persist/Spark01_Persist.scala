package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_Persist {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("input/word.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne = words.map(word => (word, 1))
        val wordToSum = wordToOne.reduceByKey(_+_)
        wordToSum.collect().foreach(println)
        val lines1: RDD[String] = sc.textFile("input/word.txt")
        val words1: RDD[String] = lines1.flatMap(_.split(" "))
        val wordToOne1 = words1.map(word => (word, 1))
        val group = wordToOne1.groupByKey()
        println("***********************")
        group.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
