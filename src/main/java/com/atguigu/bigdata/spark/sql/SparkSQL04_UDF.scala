package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL04_UDF {

    def main(args: Array[String]): Unit = {

        // TODO SparkSQL
        // 创建环境
        val spark : SparkSession =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
        val df: DataFrame = spark.read.json("input/users.json")

        df.createOrReplaceTempView("user")

        // 创建聚合函数
        val udaf = new MyAvg

        spark.udf.register("myAvg",udaf)

        // UDF函数称之为用户自定义函数，但是这个函数是不能聚合的
        // 只能对每一条进行处理。
        // zhangsan => "Name : zhangsan"
        // 如果想要使用sql完成聚合功能，那么必须采用特殊的函数：自定义聚合函数UDAF

        spark.sql("select myAvg(age) from user").show

        // 关闭环境
        spark.stop()

    }
    // 自定义年龄平均值的聚合函数
    // 继承UserDefinedAggregateFunction类
    // 重写方法
    class MyAvg extends UserDefinedAggregateFunction {
        // 输入数据的结构（年龄）
        override def inputSchema: StructType = {
            StructType(Array(StructField("age", LongType)))
        }

        // 缓冲区数据的结构（年龄总和，用户的数量）
        override def bufferSchema: StructType = {
            StructType(Array(
                StructField("total", LongType),
                StructField("count", LongType)
            ))
        }

        // 聚合函数的输出结构类型
        override def dataType: DataType = LongType

        // 稳定性
        override def deterministic: Boolean = true

        // 缓冲区初始化操作
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0, 0L)
            buffer.update(1, 0L)
        }

        // 用户输入的值更新缓冲区的值
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) + 1)
        }

        // 合并缓冲区的数据
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        // 计算聚合函数的结果
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0) / buffer.getLong(1)
        }
    }
}
