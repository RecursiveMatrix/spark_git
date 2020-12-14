package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                1,2,3,4
            )
        )

        //val rdd1: RDD[(Int, Int)] = rdd.map((_,1)).reduceByKey(_+_)
        val i: Int = rdd.reduce(_+_)
        println(i)

        sc.stop()
    }
}
