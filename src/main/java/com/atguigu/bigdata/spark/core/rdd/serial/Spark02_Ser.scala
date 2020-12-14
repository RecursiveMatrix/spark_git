package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Ser {

    def main(args: Array[String]): Unit = {

        // TODO 序列化
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc : SparkContext = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List("Hello", "Scala", "Spark"))

        val search = new Search("S")
        val newRDD: RDD[String] = search.getQueryData1(rdd)
        newRDD.collect().foreach(println)

        sc.stop()
    }
    // 类的构造参数用于赋值给对象的属性，所以外部无法访问，需要访问对象的属性才可以
    // 样例类在编译时自动混入可序列化特质
    case class Search(q:String) {

        def filterFunction( s:String ): Boolean = {
            s.contains(q)
        }

//        def getQueryData( rdd : RDD[String] ): RDD[String] = {
//            rdd.filter(this.filterFunction)
//        }
        def getQueryData1( rdd : RDD[String] ): RDD[String] = {
            val qq : String = q
            rdd.filter( s => s.contains(qq) )
        }
    }
}
