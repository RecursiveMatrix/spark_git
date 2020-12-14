package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Ser {

    def main(args: Array[String]): Unit = {

        // TODO 序列化
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd =sc.makeRDD(List[Int](),2)

        val user = new User()

        // SparkException: Task not serializable
        // 分布式执行时，需要考虑传输数据的序列化问题。
        // 分布式执行计算前，需要进行闭包检测操作，用于判断闭包操作中数据是否能够序列化
        // 如果数据不能序列化，不需要执行作业就会发生错误（提示）
       // rdd.collect().foreach(println)
        rdd.foreach(
            num => {
                println(num + user.age)
            }
        )


        sc.stop()
    }
    class User extends Serializable {
        val age : Int = 30
    }
}
