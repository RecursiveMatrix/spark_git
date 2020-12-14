package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)

        //rdd.reduce(_+_)
        //rdd.aggregate(_+_)
        //rdd.sum()
        var sum = 0

        rdd.foreach(
            num => {
                //println("num = " + num)
                sum = sum + num
                //println("sum = " + sum)
            }
        )

        // A.0
        // B 3
        // C 7
        // D 10
        println(sum)

        sc.stop()

    }
}
