package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("input/word.txt")

        // hello spark scala
        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne = words.map(
            word => (word, 1)
        )

        // Spark可以将分组和聚合合二为一
        // reduceByKey的功能可以对相同的key进行value的reducec聚合
        // spark中所有的byKey的方法必须保证处理的数据为KV类型的数据
        // (word, 1), (word, 1), (word, 1),(word, 1)
        //wordToOne.reduceByKey((x, y)=>x + y)
        val wordToSum = wordToOne.reduceByKey(_+_)

        wordToSum.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
