package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Oper_KV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO :将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值

        // 【("a", 88), ("b", 95), ("a", 91)】
        // 【("b", 93), ("a", 95), ("b", 98)】
        val rdd = sc.makeRDD(
            List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),
            2
        )

        //rdd.groupByKey()
        //rdd.groupBy
        //rdd.reduceByKey()
        //rdd.aggregateByKey()()
        //rdd.foldByKey()
        // (88, 1) + 91 => (179, 2) + 95 => (274, 3)
        // (88, 1) + 91 => (179, 2) + 95 => (274, 3)  => (548,6)
        // combineByKey算子有三个参数
        //   第一个参数表示将相同key的第一个value进行转换。
        //   第二个参数表示分区内计算规则
        //   第三个参数表示分区间计算规则
        val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey(
            (num: Int) => (num, 1),
            (t: (Int, Int), num: Int) => {
                (t._1 + num, t._2 + 1)
            },
            (t1: (Int, Int), t2: (Int, Int)) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            },
        )

        rdd1.map{
            case ( word, (total, cnt) ) => {
                ( word, total / cnt )
            }
        }.collect().foreach(println)






        sc.stop()
    }
}
