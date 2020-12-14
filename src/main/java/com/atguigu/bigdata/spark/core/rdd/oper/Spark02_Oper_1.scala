package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4),2) // ParallelCollectionRDD

        // 多个RDD所形成的依赖关系中，每一条数据必须全部的功能执行完毕后，才能继续执行后续操作
        val rdd1 = rdd.map(
            num => {
                println(num + " ******************** ")
                num
            }
        )

        val rdd2 = rdd1.map(
            num => {
                println(num + " >>>>>>>>>>>> ")
                num
            }
        )
        rdd2.collect//.foreach(println)

        sc.stop()
    }
}
