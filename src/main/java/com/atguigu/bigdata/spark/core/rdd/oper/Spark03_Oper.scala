package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
        // mapPartitions算子用于将一个分区的数据同时进行转换
        // 将一个分区可以加载到内存中进行处理，性能比较高，类似于批处理

        // mapPartitions算子依赖于内存大小。如果内存小的情况下，不推荐使用
        // 算子中处理的每一条数据处理完毕后，并不会被回收掉，只有整个分区的所有数据全部处理完毕，才会回收
        // 内存可能会溢出
        val rdd1: RDD[Int] = rdd.mapPartitions(
            iter => {
                println("************")
                iter.map(_ * 2)
            }
        )
        rdd1.collect.foreach(println)

        sc.stop()
    }
}
