package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 使用Spark框架完成WordCount

        // 从文件中获取数据，完成单词次数的统计
        // TODO 1. 获取Spark的环境
        // Connection => DriverManager.getConnection
        // conn.prepareStatment
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // TODO 2. 读取文件中的数据
        val lines: RDD[String] = sc.textFile("data/word.txt")

        // TODO 3. 将一行一行的字符串数据拆分成一个一个的单词
        // 扁平化
        val words: RDD[String] = lines.flatMap(_.split(" "))

        // TODO 4. 将相同的单词分在一个组中
        val groupRDD: RDD[(String, Iterable[String])] = words.groupBy(word=>word)

        // TODO 5. 将分组后的数据进行结构的转换
        //   map( word -> list ) => map( word -> count )
        val mapRDD = groupRDD.map{
            case ( word, list ) => {
                (word, list.size)
            }
        }

        // TODO 6. 将结果展示在控制台上
        mapRDD.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()

    }
}
