package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("input/word.txt")

        // hello spark scala
        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne = words.map(
            word => (word, 1)
        )

        val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(_._1)

        val wordToSum: RDD[(String, Int)] = groupRDD.mapValues(
            iter => {
                val ints: Iterable[Int] = iter.map(_._2)
                ints.sum
            }
        )
        wordToSum.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
