package com.atguigu.bigdata.spark.core.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Oper {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

        // 0.6 < 0.5
        // 0.3 < 0.5
        // 0.4 > 0.5

        // TODO 从数据源中抽取一部分数据
        //      采样
        // sample算子需要传递三个参数
        // 第一个参数表示抽取数据后是否放回到数据集中
        //    true : 放回，false : 不放回
        // 第二个参数基于第一个参数来判断的
        //   如果是抽取放回的场合，表示期望抽取的次数
        //   如果是抽取不放回的场合，表示抽取数据的概率
        //   每条数据的抽取的概率
        // 第三个参数表示抽取数据的种子(随机数),如果不传递，其实就是当前的系统时间
        //   随机数不随机
        val rdd1 = rdd.sample(false,0)
        val rdd2 = rdd.sample(false,1)
        val rdd3 = rdd.sample(false,0.5,1)
        val rdd4 = rdd.sample(false,0.5,1)
        val rdd5 = rdd.sample(false,0.5,2)

        //rdd1.collect().foreach(println)
        println("***************************")
        //rdd2.collect().foreach(println)
        println("***************************")
        rdd3.collect().foreach(println)
        println("***************************")
        rdd4.collect().foreach(println)
        println("***************************")
        rdd5.collect().foreach(println)



        sc.stop()
    }
}
