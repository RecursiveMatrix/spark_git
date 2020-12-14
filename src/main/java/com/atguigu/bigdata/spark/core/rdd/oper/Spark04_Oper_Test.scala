package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
        // Iterator
        val rdd1 = rdd.mapPartitionsWithIndex{
            case (index, iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                    Nil.iterator
                }
            }
        }
        rdd1.collect.foreach(println)

        sc.stop()
    }
}
