package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Oper_KV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO Join
        // join方法用于链接2个数据源，链接条件为相同的key
        // 如果两个数据源中相同key，那么会将两个v连接在一起
        // 但是如果一个数据源中有key，另外一个没有，那么无法连接
        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2),("c", 3)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("b", 4),("a", 3),("b", 5)
            )
        )

        // join也会产生笛卡尔乘积
        val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        rdd3.collect().foreach(println)


        sc.stop()
    }
}
