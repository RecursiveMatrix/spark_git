package com.atguigu.bigdata.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Part_1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        // 13 / 3 => 4
        // 13 / 4 => 3...1
        // 3 + 1 = 4

        /*
          12@@     => 0123
          345@@    => 45678
          6789     => 9101112

         0 => [0, 4]   => 【12345】
         1 => [4, 8]   => 【】
         2 => [8, 12]  => 【6789】
         3 => [12, 13] => 【】

         1G / 2 = 512M
         local : 32M
         hdfs : 128M (block)

         // Spark文件分区，以总文件数进行分区，但是具体分几个区，和文件数量也有关系
         // 10 / 2 = 5
         */
        val rdd: RDD[String] = sc.textFile("input/1.txt",3)
        rdd.saveAsTextFile("output")



        sc.stop()
    }
}
