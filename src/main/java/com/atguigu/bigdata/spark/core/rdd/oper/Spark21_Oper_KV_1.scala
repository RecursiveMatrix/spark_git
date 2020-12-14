package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Oper_KV_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO Join

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2),("c", 3)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("b", 4),("a", 3),("d", 5)
            )
        )

        val rdd3 = rdd1.leftOuterJoin(rdd2)
        val rdd4 = rdd1.rightOuterJoin(rdd2)
        rdd3.collect().foreach(println)
        println("******************")
        rdd4.collect().foreach(println)

        sc.stop()
    }
}
