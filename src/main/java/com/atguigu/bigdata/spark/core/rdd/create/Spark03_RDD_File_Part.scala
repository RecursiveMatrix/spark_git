package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Part {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // textFile有2个参数
        // 第一个参数表示为文件路径
        // 第二个参数表示最小分区数, 有默认值，在调用时，可以不用传递
        //       math.min(defaultParallelism, 2)

        // Spark的文件切片其实采用的是Hadoop操作:

        // TODO 文件作为数据源，分区计算方式
        // 1. 计算所有的文件总的字节数
        // 2. 用总字节数除以指定的分区数量，获取每个分区应该存储的字节数
        // 3. 因为不一定能整除，所以需要计算需要多少个分区才能容纳所有的数据
        //     总的字节数 / 每个分区存储的字节数 ......余数
        //     剩余的字节数是否超过每个分区存储字节数的10% => 1.1

        // 7 / 2 = 3
        // 7 / 3 = 2 ...1
        // 分区数量 = 2 + 1 = 3

        // TODO 文件作为数据源，分区数据如何存储
        // 1. 底层采用的是Hadoop操作，一行一行地读取
        // 2. 加载（读取）数据采用偏移量
        // 3. 相同偏移量的数据不会重复读取

        /*

           1@@   => 012
           2@@   => 345
           3     => 6

         */

        // 0 => [0, 3] => 【1，2】
        // 1 => [3, 6] => 【3】
        // 2 => [6, 7] => 【】

        val rdd: RDD[String] = sc.textFile("input/1.txt",2)
        rdd.saveAsTextFile("output")



        sc.stop()
    }
}
