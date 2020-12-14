package com.atguigu.bigdata.spark.core.framework.controller

import com.atguigu.bigdata.spark.core.framework.bean.HotCategoryAnalysis
import com.atguigu.bigdata.spark.core.framework.common.CommonController
import com.atguigu.bigdata.spark.core.framework.service.{HotCategorySessionTop10Service, HotCategorySessionTop10Service_1, HotCategoryTop10Service_3}

/**
  * 热门品类Top10控制器
  */
class HotCategorySessionTop10Controller extends CommonController{

    private val hotCategorySessionTop10Service = new HotCategorySessionTop10Service_1
    private val hotCategoryTop10Service = new HotCategoryTop10Service_3

    override def execute(): Unit = {
        val top10: List[HotCategoryAnalysis] = hotCategoryTop10Service.analysis()
        val result = hotCategorySessionTop10Service.analysis(top10)
        result.foreach(println)
    }
}
