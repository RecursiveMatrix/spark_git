package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                "Hello Scala", "Hello Spark"
            )
        )
        // flatMap算子可以将数据源中的每一条数据进行扁平映射操作
        def flatMapFunction( s : String ): Array[String] = {
            s.split(" ")
        }

        //val rdd1 = rdd.flatMap(flatMapFunction)
        //val rdd1 = rdd.flatMap(( s : String )=>{s.split(" ")})
        //val rdd1 = rdd.flatMap(( s : String )=>s.split(" "))
        //val rdd1 = rdd.flatMap(( s )=>s.split(" "))
        //val rdd1 = rdd.flatMap(s=>s.split(" "))
        val rdd1 = rdd.flatMap(_.split(" "))

        rdd1.collect().foreach(println)



        sc.stop()
    }
}
