package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_Oper_KV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                ("b", 5),("a",1),("b", 2),("a", 4),("a", 3)
            )
        )

        // sortByKey根据数据的key进行排序，和value无关
        // (b,2),(b,5)(a,1)(a,3)(a,4)
        //val rdd1 = rdd.sortByKey(false)

        // sortBy根据指定的规则进行排序
        val rdd1: RDD[(String, Int)] = rdd.sortBy(_._1)
        rdd1.collect().foreach(println)








        sc.stop()
    }
}
