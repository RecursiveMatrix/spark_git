package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark07_Acc_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(
                ("a",1),("a",2),("b",3),("b", 4)
            )
        )

        // TODO 1. 创建累加器
        val acc = new WordCountAccumulator()
        // TODO 2. 向Spark进行注册
        sc.register(acc, "WordCount")

        rdd
//        .map{
//            case (k, v) => {
//                (k, v * 2)
//            }
//        }
        //.reduceByKey(_+_)
        .foreach(
            kv => {
                // TODO 3. 向累加器中增加数据
                acc.add(kv)
            }
        )

        // TODO 4. 获取累加器的累加结果
        println(acc.value)
        sc.stop()


    }
    class WordCountAccumulator extends AccumulatorV2[(String, Int), mutable.Map[String, Int]]{

        var wcmap = mutable.Map[String, Int]() // Map.apply() => Map

        // TODO 判断累加器是否为初始状态
        //  copyAndReset must return a zero value copy
        override def isZero: Boolean = {
            wcmap.isEmpty
        }

        // TODO 复制累加器
        //      累加器copy方法的调用次数和分区数量无关，而是和序列化的次数有关
        override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = {
            //println("copy.....")
            new WordCountAccumulator()
        }

        // TODO 重置累加器
        override def reset(): Unit = {
            wcmap.clear()
        }

        // TODO 向累加器中增加数据
        override def add(t: (String, Int)): Unit = {
            // map + kv
            val k = t._1
            val v = t._2

            wcmap.update(k, wcmap.getOrElse(k, 0) + v)
        }

        // TODO 合并多个累加器的值
        override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {

            var map1 = this.wcmap
            var map2 = other.value

            this.wcmap = map1.foldLeft(map2) {
                case ( m, (k, v) ) => {
                    m.updated(k, m.getOrElse(k, 0) + v)
                }
            }
        }

        // TODO 返回累加器的计算结果
        override def value: mutable.Map[String, Int] = {
            wcmap
        }
    }
}
