package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.CommonApplication
import com.atguigu.bigdata.spark.core.framework.controller.{HotCategorySessionTop10Controller, HotCategoryTop10Controller}

object HotCategorySessionTop10Application extends CommonApplication with App {

    startApp(appName="HotCategorySessionTop10") {
        val controller = new HotCategorySessionTop10Controller
        controller.execute()
    }
}
