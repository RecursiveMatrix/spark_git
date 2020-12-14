package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Oper_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

        val rdd1 = rdd.sample(true, 2)

        rdd1.collect().foreach(println)



        sc.stop()
    }
}
