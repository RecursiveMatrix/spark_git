package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Persist {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)
        sc.setCheckpointDir("cp")
        val lines: RDD[String] = sc.textFile("input/word.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne = words.map(word => {
            println("word = " + word)
            (word, 1)
        })

        // TODO rdd的检查点
        //  Checkpoint directory has not been set in the SparkContext
        // 检查点保存的数据可以给其他的app使用
        // 1. cache & persist 不会重复执行
        // 2. checkpoint会重复执行，当调用检查点操作时，会额外启动一个Job执行
        //    一般情况下，checkpoint应该和cache联合使用

        // 血缘关系：
        // 1. checkpoint会切断RDD的血缘关系
        // 2. cache是在血缘关系中添加了一个缓存依赖

        // checkpoint使用的其实不多，因为可能会产生大量的小文件。
        wordToOne.cache()
        //wordToOne.checkpoint()
        println(wordToOne.toDebugString)
        val wordToSum = wordToOne.reduceByKey(_+_)
        wordToSum.collect().foreach(println)
        println("*********************************")
        println(wordToOne.toDebugString)

        // TODO 关闭环境
        sc.stop()

    }
}
