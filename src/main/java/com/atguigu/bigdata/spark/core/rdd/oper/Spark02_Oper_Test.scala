package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // 从服务器日志数据apache.log中获取用户请求URL资源路径
        val rdd = sc.textFile("input/apache.log")
        // xxxxxxxxxxxxxxx => yyyyyy
        val urls = rdd.map(
            line => {
                val datas = line.split(" ")
                datas(6)
            }
        )
        urls.collect.foreach(println)

        sc.stop()
    }
}
