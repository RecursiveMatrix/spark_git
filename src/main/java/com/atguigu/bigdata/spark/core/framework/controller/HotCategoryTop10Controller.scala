package com.atguigu.bigdata.spark.core.framework.controller

import com.atguigu.bigdata.spark.core.framework.common.CommonController
import com.atguigu.bigdata.spark.core.framework.service.{HotCategoryTop10Service, HotCategoryTop10Service_1, HotCategoryTop10Service_2, HotCategoryTop10Service_3}

/**
  * 热门品类Top10控制器
  */
class HotCategoryTop10Controller extends CommonController{

    private val hotCategoryTop10Service = new HotCategoryTop10Service_3

    override def execute(): Unit = {
        val result = hotCategoryTop10Service.analysis()
        result.foreach(println)
    }
}
