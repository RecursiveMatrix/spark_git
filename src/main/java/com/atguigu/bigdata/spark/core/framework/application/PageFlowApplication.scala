package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.CommonApplication
import com.atguigu.bigdata.spark.core.framework.controller.{HotCategorySessionTop10Controller, PageFlowController}

object PageFlowApplication extends CommonApplication with App {

    startApp(appName="PageFlow") {
        val controller = new PageFlowController
        controller.execute()
    }
}
