package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object SparkSQL07_Hive {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "root")
        // TODO SparkSQL
        // 创建环境
        val spark : SparkSession =
            SparkSession.builder()
                .enableHiveSupport() // 启用Hive的支持
                .master("local[*]").appName("SparkSQL").getOrCreate()
        import spark.implicits._

        spark.sql("show tables").show

        // 链接外置的Hive

        // 关闭环境
        spark.stop()

    }
}
