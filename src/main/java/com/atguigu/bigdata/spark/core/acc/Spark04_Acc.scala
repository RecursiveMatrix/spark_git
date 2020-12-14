package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Acc {

    def main(args: Array[String]): Unit = {

        var sum = 0

        def test(): Unit = {
            for ( i <- 1 to 5 ) {
                sum += i
            }
        }

        val f1 = test _

        f1()
        f1()

        println(sum)


    }
}
