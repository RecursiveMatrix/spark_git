package com.atguigu.bigdata.spark.core.framework.controller

import com.atguigu.bigdata.spark.core.framework.bean.HotCategoryAnalysis
import com.atguigu.bigdata.spark.core.framework.common.CommonController
import com.atguigu.bigdata.spark.core.framework.service.{HotCategorySessionTop10Service_1, HotCategoryTop10Service_3, PageFlowService}

/**
  * 热门品类Top10控制器
  */
class PageFlowController extends CommonController{

    private val pageFlowService = new PageFlowService

    override def execute(): Unit = {
        val result = pageFlowService.analysis()
        result
    }
}
