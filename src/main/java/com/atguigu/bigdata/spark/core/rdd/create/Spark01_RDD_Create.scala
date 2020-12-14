package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Create {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)
        // RDD的创建一般采用从内存，从磁盘中创建
        // RDD : 弹性，分布式，数据集
        //       对指定数据集（源）数据处理的数据模型
        // RDD需要处理的数据源可能来自于内存，也可能来自文件

        // TODO 从内存中加载数据源
        // parallelize : 并行, 可以从内存数据源创建RDD
        // k8s
        // i18n
        val seq = Seq(1,2,3,4)
        val list = List(1,2,3,4)

        val rdd: RDD[Int] = sc.parallelize(seq)
        // makeRDD的底层调用就是parallelize，就是为了使用方便所提供的方法
        val rdd1: RDD[Int] = sc.makeRDD(list)
        val rdd2: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        rdd1.collect()

        sc.stop()
    }
}
