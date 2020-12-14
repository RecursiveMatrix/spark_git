package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        //sparkConf.set("spark.local.dir", "D:\\ttt")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)
        rdd.saveAsTextFile("output")

        val rdd1: RDD[(Int, Iterable[Int])] = rdd.groupBy(
            (num:Int)=> {
                num%2
            },3
        )
        rdd1.saveAsTextFile("output1")



        sc.stop()
    }
}
