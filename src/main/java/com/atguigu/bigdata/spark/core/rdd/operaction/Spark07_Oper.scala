package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper {

    def main(args: Array[String]): Unit = {

        val list = List(
            ("Hello", 4)
        )
        val tuples: List[(String, Int)] = list.flatMap {
            case (w, c) => {
                // (Hello,1) (Hello Hello Hello
                //val s = (w + " ") * c
                //s.split(" ").map((_, 1))
                Array.fill[String](c)(w).map((_,1))
            }
        }
        println(tuples)

    }
}
