package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL05_UDAF_Class_1 {

    def main(args: Array[String]): Unit = {

        // TODO SparkSQL
        // 创建环境
        val spark : SparkSession =
            SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("input/users.json")
        // 创建聚合函数
        val udaf = new MyAvg

        // Spark3.0版本前，无法将强类型聚合函数使用在SQL文中
        // 早期版本中，将数据的一行当成对象传递个聚合函数
        // DSL
        val ds: Dataset[User] = df.as[User]

        // 将聚合函数转换为查询列
        ds.select(udaf.toColumn).show

        // 关闭环境
        spark.stop()

    }
    case class User( id:Long, age:Long, name:String )
    case class AvgBuffer( var total:Long, var count:Long )
    // 自定义年龄平均值的聚合函数(强类型)
    // 1. 继承org.apache.spark.sql.expressions.Aggregator
    // 2. 定义泛型
    //    IN : User(将一行数据作为输入)
    //    BUF : AvgBuffer
    //    OUT : Long
    // 3. 重写方法
    class MyAvg extends Aggregator[User, AvgBuffer, Long] {

        override def zero: AvgBuffer = {
            AvgBuffer(0L, 0L)
        }

        override def reduce(b: AvgBuffer, user: User): AvgBuffer = {
            b.total += user.age
            b.count += 1
            b
        }

        override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
            b1.total += b2.total
            b1.count += b2.count
            b1
        }

        override def finish(reduction: AvgBuffer): Long = {
            reduction.total / reduction.count
        }

        override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
