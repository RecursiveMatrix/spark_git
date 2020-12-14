package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO
        //   Spark提供了一种特殊的数据结构，用于通知Executor在计算完毕后的数据返回到Driver
        //   Driver会将多个Executor计算的结果合并在一起，获取最终的结果
        //   这种数据结构称之为数据采集器（累加器）

        // TODO 1. 声明累加器
        val sum: LongAccumulator = sc.longAccumulator("sum")

        // TODO
        //   Spark默认提供的累加器有3种类型
        //   1. LongAccumulator : 整型
        //   2. DoubleAccumulator : 浮点类型
        //   3. CollectionAccumulator ：List集合类型


        val rdd = sc.makeRDD(List(1,2,3,4),2)

        rdd.foreach(
            num => {
                // TODO 2. 使用累加器
                sum.add(num)
            }
        )

        // TODO 3. 获取累加器的结果
        println(sum.value)

        sc.stop()

        // Map[(word, count)], Map[(word, count)], Map[(word, count)]


    }
}
