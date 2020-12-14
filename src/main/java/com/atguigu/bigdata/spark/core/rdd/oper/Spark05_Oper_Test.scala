package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Oper_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                List(1,2),
                3,
                List(4,5),
            )
        )

        val rdd1 = rdd.flatMap{
            case list:List[_] => list
            case d => List(d)
        }

        rdd1.collect().foreach(println)



        sc.stop()
    }
}
