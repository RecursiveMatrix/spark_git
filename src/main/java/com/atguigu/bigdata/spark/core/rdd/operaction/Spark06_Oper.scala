package com.atguigu.bigdata.spark.core.rdd.operaction

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                1,2,1,1
            ),
            2
        )

        // 将数据保存到分区文件中
        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        rdd.map((_,1)).saveAsSequenceFile("output2")

        sc.stop()
    }
}
