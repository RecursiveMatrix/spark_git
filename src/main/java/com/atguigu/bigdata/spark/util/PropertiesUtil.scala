package com.atguigu.bigdata.spark.util

import java.util.ResourceBundle

object PropertiesUtil {

    // ResourceBundle类专门用于读取配置文件（properties）
    // k8s
    // i18n => 国际化 => zh-CN
    private val rb : ResourceBundle = ResourceBundle.getBundle("test")
    def getValue( key : String ): String = {
        rb.getString(key)
    }

    def main(args: Array[String]): Unit = {
        println(getValue("kafka.broker"))
    }
}
