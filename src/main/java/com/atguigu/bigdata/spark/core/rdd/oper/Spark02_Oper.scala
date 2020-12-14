package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4),2) // ParallelCollectionRDD
        rdd.saveAsTextFile("output")
        // TODO RDD - 转换算子 - map
        // map : 转换，将数据源中的每一个数据转换成其他数据返回

        // 转换算子默认情况下，创建新的RDD时，分区数量不变
        // 分区数据处理后所在的分区也不会发生变化
//        def mapFunction( num:Int ): Int = {
//            num * 2
//        }
        //val mapRDD: RDD[Int] = rdd.map(mapFunction)
        //val mapRDD: RDD[Int] = rdd.map(( num:Int )=>{num * 2})
        //val mapRDD: RDD[Int] = rdd.map(( num:Int )=>num * 2)
        //val mapRDD: RDD[Int] = rdd.map(( num )=>num * 2)
        //val mapRDD: RDD[Int] = rdd.map(num=>num * 2)
        //val mapRDD: RDD[Int] = rdd.map(_ * 2)
        //mapRDD.collect().foreach(println)
        val mapRDD: RDD[Int] = rdd.map(
            num => {
                println("**********")
                num * 2
            }
        )

        //mapRDD.saveAsTextFile("output1")
        mapRDD.collect()

        sc.stop()
    }
}
