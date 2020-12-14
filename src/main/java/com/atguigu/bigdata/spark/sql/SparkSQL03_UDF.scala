package com.atguigu.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_UDF {

    def main(args: Array[String]): Unit = {

        // TODO SparkSQL
        // 创建环境
        val spark : SparkSession =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()

        import spark.implicits._
        val df: DataFrame = spark.read.json("input/users.json")

        df.createOrReplaceTempView("user")

        spark.udf.register("prefixName", (name:String)=>{ "Name : " + name })

        spark.sql("select prefixName(name) from user").show

        // 关闭环境
        spark.stop()

    }
    case class User(id:Int, name:String, age:Int)
}
