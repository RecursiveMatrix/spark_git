package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Bc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("a", 3), ("b", 4)
            )
        )

        // join算子的底层调用cogroup
        val rdd3 = rdd1.join(rdd2)

        // ( a, (1,3) ), ( b, (2,4) )
        // join算子不推荐使用
        // 1. 笛卡尔乘积 => 数据量几何性增长
        // 2. shuffle
        println(rdd3.collect().mkString(","))

        sc.stop()

    }
}
