package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,3,5,4,2,6), 3)

        val rdd1 = rdd.sortBy(num=>num, true)

        rdd1.saveAsTextFile("output")

        //List(1,2,3,4).sortBy()()


        sc.stop()
    }
}
