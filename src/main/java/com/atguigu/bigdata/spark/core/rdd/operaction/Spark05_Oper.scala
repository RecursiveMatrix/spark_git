package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                1,2,1,1
            ),
            2
        )

        // countByValue可以实现WordCount  7/10
        //val intToLong: collection.Map[Int, Long] = rdd.countByValue()
        //println(intToLong)

        // countByKey可以实现WordCount  8/10
        val rdd1 = sc.makeRDD(
            List(
                ("Hello", 4), ("Hello", 3)
            )
        )
//        val stringToLong: collection.Map[String, Long] = rdd.map(num=>(num.toString, num)).countByKey()
//        println(stringToLong)
        val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
        println(stringToLong)
        // ("Hello", 4)("Hello", 4)("Hello", 4)("Hello", 4), ("Hello", 3)("Hello", 3)("Hello", 3) => ("Hello", 7)

        // ("Hello", 4)
        // ("Hello", 1),("Hello", 1)("Hello", 1)("Hello", 1)

        sc.stop()
    }
}
