package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_Oper_KV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // Spark的RDD处理的数据类型，如果为KV类型，需要进行特殊的转换才能调用特殊的功能
        val kvRDD : RDD[(Int, Int)] = rdd.map((_,1))
        // 所有的kv数据的操作方法全部都来自于PairRDDFunctions
        // 使用的是scala中的隐式转换语法

        // TODO partitionBy方法根据指定的规则对数据进行重分区
        //rdd.repartition() // repartition主要目的是改变分区的数量
        // partitionBy需要传递一个分区器对象，改变数据所在的分区
        // Partitioner分区器对象有2个具体的分区器，HashPartitioner & RangePartitioner
        // spark中很多的RDD操作默认的分区器都是HashPartitioner
        val rdd1 = kvRDD.partitionBy(new HashPartitioner(2)) // partitionBy主要目的是改变数据所存放的分区
        val rdd2 = rdd1.partitionBy(new HashPartitioner(2)) // partitionBy主要目的是改变数据所存放的分区

        rdd1.saveAsTextFile("output")


        sc.stop()
    }
}
