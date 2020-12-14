package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.LongAccumulator

object Spark05_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val sum: LongAccumulator = sc.longAccumulator("sum")
        //var sum = 0

        // 累加器在多次行动算子执行时，数据可能计算有问题。
        // 一般推荐累加器在行动算子中执行，如果非要在转换算子中使用，需要保证行动算子只会执行一次
        val rdd = sc.makeRDD(1 to 10,2)

        val rdd1 = rdd.filter(
            num => {
                val flg = num % 2 != 0
                if ( !flg ) {
                    sum.add(1)
                }
                flg
            }
        )

        //rdd1.collect()
        rdd1.foreach(println)
        println("***********************************")
        // TODO 3. 获取累加器的结果
        println(sum.value)

        sc.stop()
    }
}
