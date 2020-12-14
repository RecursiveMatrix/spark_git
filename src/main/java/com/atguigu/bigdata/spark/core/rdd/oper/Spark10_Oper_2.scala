package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Oper_2 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,1,2,1,2))

        // rdd.map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
        // 1 => (1, null)
        // 1 => (1, null)
        // 1 => (1, null)
        //      (1, null) => 1
        val rdd1 = rdd.distinct()


        rdd1.collect().foreach(println)


        sc.stop()
    }
}
