package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        sc.textFile("input/word.txt")
            .flatMap(_.split(" "))
            .groupBy(word=>word)
            .map{
                case ( word, list ) => {
                    (word, list.size)
                }
            }.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
