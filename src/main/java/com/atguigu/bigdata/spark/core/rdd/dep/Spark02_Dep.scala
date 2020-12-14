package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Dep {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.makeRDD(List("a", "b", "c"))

        println(lines.dependencies) // 依赖
        println("*********************************")

        // OneToOneDependency
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("*********************************")

        // OneToOneDependency
        val wordToOne = words.map(
            word => (word, 1)
        )
        println(wordToOne.dependencies)
        println("*********************************")

        // ShuffleDependency
        val wordToSum = wordToOne.reduceByKey(_+_)
        println(wordToSum.dependencies)
        println("*********************************")

        wordToSum.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
