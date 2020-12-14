package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        //sparkConf.set("spark.local.dir", "D:\\ttt")
        val sc : SparkContext = new SparkContext(sparkConf)

        //val rdd = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"),2)
        // TODO 将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组
        //val rdd1 = rdd.groupBy(_.substring(0,1))

        // TODO 从服务器日志数据apache.log中获取每个时间段访问量
        // word -> count
        // (10, 1) => (10. sum)
        val rdd = sc.textFile("input/apache.log")

        val hourToOne = rdd.map(
            line => {
                val datas = line.split(" ")
                // 17/05/2015:10:05:03
                // 10
                val times = datas(3).split(":")
                (times(1), 1)
            }
        )

        // TODO Spark - 10 - WordCount - 1
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = hourToOne.groupBy(_._1)

        val hourToSum = groupRDD.mapValues(
            iter => {
                iter.size
            }
        )

        hourToSum.collect().foreach(println)



        sc.stop()
    }
}
