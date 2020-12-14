package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Oper {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(1,2,3,4),2
        )

        // collect是按照分区的顺序采集数据的方法
        // foreach方法其实是scala集合的方法，是单点操作，所以是按照顺序循环
        rdd.collect().foreach(println)
        println("***************")
        // rdd的所有算子其实都是分布式操作
        //所以这里的foreach操作其实它是分布式循环打印
        // 算子内部的逻辑操作全部都是在Executor端执行
        // 算子外部的逻辑操作全部都是在Driver端执行
        rdd.foreach(println)

        //rdd.sortBy()

        sc.stop()
    }
}
