package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Oper_KV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                ("a", 1), ("b", 1), ("a", 1), ("b", 1)
            ),2
        )
        // groupBy & groupByKey
        // groupBy可以根据数据来计算分组的key
        //        分组后，每一个数据都会放置在一个组中
        // groupByKey固定采用key作为分组key
        //        分组后，每一个kv数据的v会放置在一个组中
        //val rdd1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
        // groupByKey可以实现wordcounr 3/10
        val rdd2: RDD[(String, Iterable[Int])] = rdd.groupByKey()
        val rdd3 = rdd2.mapValues(
            iter=>iter.size
        )

        println(rdd3.collect().mkString(","))


        sc.stop()
    }
}
