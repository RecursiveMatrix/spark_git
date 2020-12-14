package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Create_File_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO 从文件中加载数据源
        // wholeTextFiles方法可以返回元组数据
        // 元组的第一个数据表示文件路径，第二个数据表示文件的完整内容
        val rdd: RDD[(String, String)] = sc.wholeTextFiles("input")
        rdd.collect().foreach(println)


        sc.stop()
    }
}
