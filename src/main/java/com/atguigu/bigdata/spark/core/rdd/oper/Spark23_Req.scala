package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_Req {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO 读取数据文件
        val lines = sc.textFile("input/agent.log")

        // TODO 将读取后的数据进行分解，转换结构
        // line =>（（省份，广告），1）
        val prvAndAdToOne = lines.map(
            line => {
                val datas = line.split(" ")
                (( datas(1), datas(4) ),1)
            }
        )

        // TODO 将转换结构后的数据进行分组聚合
        // （（省份，广告），1） => （（省份，广告），sum）
        val prvAndAdToSum = prvAndAdToOne.reduceByKey(_+_)

        // TODO 将聚合后的结果进行结构的转换
        // （（省份，广告），sum）=> (省份， （广告， sum）)
        val prvToAdAndSum = prvAndAdToSum.map {
            case ( (prv, ad), sum ) => {
                (prv, (ad, sum))
            }
        }

        // TODO 将转换结构后的数据根据省份进行分组
        // (省份， （广告， sum）) => （省份， Iterator[  （广告1， sum1）,（广告2， sum2） ]）
        val group: RDD[(String, Iterable[(String, Int)])] = prvToAdAndSum.groupByKey()

        // TODO 将分组后的数据进行排序（点击数量降序），取前3名 : Top3
        val result: RDD[(String, List[(String, Int)])] = group.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        result.collect().foreach(println)

        sc.stop()
    }
}
