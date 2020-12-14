package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Memory_Part_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4), 2)
        val rdd1 = sc.makeRDD(List(1,2,3,4), 4)
        val rdd2 = sc.makeRDD(List(1,2,3,4), 3)
        val rdd3 = sc.makeRDD(List(1,2,3,4,5), 3)
        // 将数据保存到分区文件
        //rdd1.saveAsTextFile("output1")
        //rdd2.saveAsTextFile("output2")
        rdd3.saveAsTextFile("output3")


        sc.stop()
    }
}
