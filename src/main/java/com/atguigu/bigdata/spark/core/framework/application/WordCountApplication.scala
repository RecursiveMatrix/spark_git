package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.CommonApplication
import com.atguigu.bigdata.spark.core.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * WordCount 应用程序
  */
object WordCountApplication extends CommonApplication with App{

    startApp(appName="WordCount") {
        val controller = new WordCountController()
        controller.execute()
    }

}
