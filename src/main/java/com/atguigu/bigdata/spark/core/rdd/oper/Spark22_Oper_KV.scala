package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark22_Oper_KV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO Join

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2),("c", 3)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("b", 4),("a", 3),("d", 5),("a",6)
            )
        )

        // 每个数据源进行groupByKey,多个数据源进行connect
        // co + group
        val rdd3 = rdd1.cogroup(rdd2)
        rdd3.collect().foreach(println)

        sc.stop()
    }
}
