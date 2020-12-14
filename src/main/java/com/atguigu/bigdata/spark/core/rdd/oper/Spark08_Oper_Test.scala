package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Oper_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.textFile("input/apache.log")
        val rdd1 = rdd.filter(
            line => {
                val datas = line.split(" ")
                datas(3).startsWith("17/05/2015")
            }
        )
        val rdd2 = rdd1.map(
            line => {
                val datas = line.split(" ")
                datas(6)
            }
        )
        rdd2.collect().foreach(println)



        sc.stop()
    }
}
