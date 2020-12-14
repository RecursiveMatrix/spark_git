package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

object SparkSQL05_UDAF_Class {

    def main(args: Array[String]): Unit = {

        // TODO SparkSQL
        // 创建环境
        val spark : SparkSession =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
        val df: DataFrame = spark.read.json("input/users.json")

        df.createOrReplaceTempView("user")

        // 创建聚合函数
        val udaf = new MyAvg

        spark.udf.register("myAvg",functions.udaf(udaf))

        // SQL本身就是弱类型操作，支持弱类型的聚合函数,不能直接支持强类型的聚合函数
        spark.sql("select myAvg(age) from user").show

        // 关闭环境
        spark.stop()

    }
    case class AvgBuffer( var total:Long, var count:Long )
    // 自定义年龄平均值的聚合函数(强类型)
    // 1. 继承org.apache.spark.sql.expressions.Aggregator
    // 2. 定义泛型
    //    IN : Long
    //    BUF : AvgBuffer
    //    OUT : Long
    // 3. 重写方法
    class MyAvg extends Aggregator[Long, AvgBuffer, Long] {
        // 缓冲区的初始操作
        override def zero: AvgBuffer = {
            AvgBuffer(0L, 0L)
        }

        // 将年龄数据和缓冲区的数据进行聚合
        override def reduce(b: AvgBuffer, age: Long): AvgBuffer = {
            b.total += age
            b.count += 1
            b
        }

        // 多个缓冲区的合并
        override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
            b1.total += b2.total
            b1.count += b2.count
            b1
        }

        // 计算结果
        override def finish(reduction: AvgBuffer): Long = {
            reduction.total / reduction.count
        }

        override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
