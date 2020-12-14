package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Bc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2)
            )
        )
        // TODO 1. 创建广播变量
        val map = Map(
            ("a", 3), ("b", 4)
        )
        val bc: Broadcast[Map[String, Int]] = sc.broadcast(map)

        val rdd3 = rdd1.map {
            case ( k, v ) => {
                // TODO 使用广播变量
                ( k, (v, bc.value.getOrElse(k, 0)) )
            }
        }

        println(rdd3.collect().mkString(","))

        sc.stop()

    }
}
