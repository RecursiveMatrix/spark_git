package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_Dep {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.makeRDD(List("a", "b", "c"))

        println(lines.toDebugString) // 血缘
        println("*********************************")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        println("*********************************")

        val wordToOne = words.map(
            word => (word, 1)
        )
        println(wordToOne.toDebugString)
        println("*********************************")

        val wordToSum = wordToOne.reduceByKey(_+_)
        println(wordToSum.toDebugString)
        println("*********************************")

        wordToSum.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
