package com.atguigu.bigdata.spark.core.rdd.save

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Save {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("a", 1), ("b", 2), ("c", 3)
        ))

        rdd.saveAsTextFile("output")
        rdd.saveAsSequenceFile("output1")
        rdd.saveAsObjectFile("output2")

        // TODO 关闭环境
        sc.stop()

    }
}
