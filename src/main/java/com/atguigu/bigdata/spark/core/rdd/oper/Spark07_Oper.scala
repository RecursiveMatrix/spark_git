package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        sparkConf.set("spark.local.dir", "D:\\ttt")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)
        //rdd.saveAsTextFile("output")
        // 分组 - groupBy
        // 分组key
        // groupBy算子用于将数据源中的每一条数据计算分组key
        // 相同的key的数据会放置在一个组中
        val rdd1: RDD[(Int, Iterable[Int])] = rdd.groupBy(num=>num%2)
        //rdd1.saveAsTextFile("output1")

        rdd1.collect().foreach(println)



        sc.stop()
    }
}
