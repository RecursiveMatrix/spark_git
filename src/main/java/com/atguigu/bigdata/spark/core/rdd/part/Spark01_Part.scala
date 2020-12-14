package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_Part {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                ("nba", "XXXXXX"),
                ("cba", "YYYYYY"),
                ("nba", "XXXXCC"),
                ("wnba", "TTXXCC")
            ),
            2
        )

        // 通过分区器决定数据所在的分区
        val rdd1 = rdd.partitionBy( new BasketBallPartitioner(2) )
        rdd1.saveAsTextFile("output")

        // TODO 关闭环境
        sc.stop()

    }
    // 自定义数据分区器
    // 1. 继承Partitioner
    // 2. 重写抽象方法
    class BasketBallPartitioner(num:Int) extends Partitioner{
        // 分区的数量
        override def numPartitions: Int = num

        // 根据数据的key来获取所在分区的位置
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case _ => 1
            }
        }
    }
}
