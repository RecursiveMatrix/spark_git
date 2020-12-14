package com.atguigu.bigdata.spark.core.framework.controller

import com.atguigu.bigdata.spark.core.framework.common.CommonController
import com.atguigu.bigdata.spark.core.framework.service.WordCountService
import org.apache.spark.SparkContext

/**
  * WordCount控制器
  */
class WordCountController extends CommonController {
    private val wordCountService = new WordCountService()

    // 所有的控制器都应该有execute方法
    override def execute(): Unit = {
        val wordToSum = wordCountService.analysis()
        wordToSum.foreach(println)
    }
}
