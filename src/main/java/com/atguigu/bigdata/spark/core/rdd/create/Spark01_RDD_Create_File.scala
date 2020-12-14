package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Create_File {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO 从文件中加载数据源
        // 从文件中加载数据源，需要设定文件路径，这个路径可以是绝对路径，也可以是相对路径
        // 返回的RDD[String]表示后续操作的数据类型为字符串，这里的字符串其实就是文件每一行数据
        // Spark读取文件采用的是Hadoop读取的原理
        // 文件路径可以读取本地文件，也可以读取HDFS中存储的文件
        // 到底读取本地文件还是读取HDFS文件，可以根据运行环境来确定
        //val rdd: RDD[String] = sc.textFile("input/word.txt")
        // 文件路径可以是具体的文件路径，也可以是文件目录
        val rdd: RDD[String] = sc.textFile("input")
        rdd.collect().foreach(println)


        sc.stop()
    }
}
