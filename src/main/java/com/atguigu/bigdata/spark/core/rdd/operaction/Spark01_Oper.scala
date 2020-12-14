package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                1,2,3,4
            )
        )

        val rdd1 = rdd.map(
            num => {
                println("*********")
                num * 2
            }
        )
        // collect方法其实可以触发作业（计算）的执行
        // collect算子可以触发作业的执行，是因为在运行中，会动态创建Job对象
        // 算子的每一次调用，都会创建一个作业（Job）
        rdd1.collect()
        rdd1.collect()

        sc.stop()
    }
}
