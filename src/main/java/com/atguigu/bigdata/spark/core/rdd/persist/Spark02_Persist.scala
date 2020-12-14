package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Persist {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("input/word.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne = words.map(word => {
            println("word = " + word)
            (word, 1)
        })

        // TODO rdd的持久化 - 缓存
        // cache只能将数据保存到内存中，数据不够安全
        wordToOne.cache()
        // TODO rdd的持久化
        // persist方法可以将数据保存到内存中，也可以将数据保存到磁盘中
        // persist方法默认将数据保存到内存中，如果想要保存到磁盘中，需要更改存储级别


        // cache缓存和persist持久化操作都是以app为单位,当app执行完毕后，落盘或内存中的数据会删除
        wordToOne.persist(StorageLevel.DISK_ONLY)

        val wordToSum = wordToOne.reduceByKey(_+_)
        wordToSum.collect().foreach(println)

        val group = wordToOne.groupByKey()
        println("***********************")
        group.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
