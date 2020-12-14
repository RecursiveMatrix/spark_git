package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)

        // TODO 过滤 - Filter
        // 按照指定的规则对数据进行筛选过滤，会根据返回的结果判断是否保留
        // true, 保留， false，丢弃
        val rdd1 = rdd.filter(
            num => {
                num % 2 != 0
            }
        )


        rdd1.collect().foreach(println)



        sc.stop()
    }
}
