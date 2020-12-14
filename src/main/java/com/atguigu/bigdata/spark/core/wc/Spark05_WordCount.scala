package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("a",1),("a",2),("a",2),("b",1),("b",2)
        ))
        // [(a, 1)], [(a, 2)],[(a, 3)]
        val wordToCount = rdd.map(
            t => {
                mutable.Map[String, Int](t)
            }

        ).reduce(
            (m1, m2) => {
                // 两个map的合并
                m1.foldLeft(m2) {
                    case ( m, (k, v) ) => {

//                        val oldValue = m.getOrElse(k, 0)
//                        val newValue = oldValue + v
//                        m.update(k, newValue)
//                        m

                        m.updated(k, m.getOrElse(k, 0) + v)
                    }
                }
            }
        )

        println(wordToCount)

        // TODO 关闭环境
        sc.stop()

    }
}
