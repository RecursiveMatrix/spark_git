package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_Oper_KV_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                (new User(), 5),(new User(),1),(new User(), 2),(new User(), 4),(new User(), 3)
            )
        )
        val rdd1 = rdd.sortByKey(true)
        rdd1.collect().foreach(println)

        sc.stop()
    }
    class User extends Ordered[User]{
        override def compare(that: User): Int = {
            0
        }
    }
}
