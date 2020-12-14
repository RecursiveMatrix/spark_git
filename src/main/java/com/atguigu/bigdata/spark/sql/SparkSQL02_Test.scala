package com.atguigu.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL02_Test {

    def main(args: Array[String]): Unit = {

        // TODO SparkSQL
        // 创建环境
        val spark : SparkSession =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
        import spark.implicits._

        // DataFrame & Dataset
        // type DataFrame = Dataset[Row]

        // DataFrame其实就是Dataset一个特例，当Dataset的泛型为Row类型，就可以采用DataFrame代替
        val df1: DataFrame = spark.read.json("input/users.json")
        val df2: Dataset[Row] = spark.read.json("input/users.json")

        val rdd1: RDD[Row] = df1.rdd
        val rdd2: RDD[Row] = df2.rdd

        //df1.filter(x=>false)

        // DataFrame的map方法类似于SparkCore中RDD的转换算子，只不过转换地为执行计划
        //df1.map(r=>r)
        //rdd1.map(r=>r)

        // DataFrame的show方法类似于SparkCore中RDD的行动算子
        df1.show



        // 关闭环境
        spark.stop()

    }
    case class User(id:Int, name:String, age:Int)
}
