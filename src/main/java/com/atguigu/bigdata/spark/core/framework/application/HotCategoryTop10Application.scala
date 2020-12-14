package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.CommonApplication
import com.atguigu.bigdata.spark.core.framework.controller.HotCategoryTop10Controller

object HotCategoryTop10Application extends CommonApplication with App {

    startApp(appName="HotCategoryTop10") {
        val controller = new HotCategoryTop10Controller
        controller.execute()
    }
}
