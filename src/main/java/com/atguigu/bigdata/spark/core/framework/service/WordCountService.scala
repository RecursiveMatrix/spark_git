package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.common.CommonService
import com.atguigu.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
  * WordCount服务对象
  */
class WordCountService extends CommonService{

    private val wordCountDao = new WordCountDao()
    /**
      * 数据分析
      */
    override def analysis() = {
        val lines = wordCountDao.getFileData("input/word.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne = words.map(
            word => (word, 1)
        )
        val wordToSum = wordToOne.reduceByKey(_+_)
        wordToSum.collect()
    }
}
