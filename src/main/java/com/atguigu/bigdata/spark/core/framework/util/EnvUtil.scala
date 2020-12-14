package com.atguigu.bigdata.spark.core.framework.util

import org.apache.spark.{SparkConf, SparkContext}

object EnvUtil {

    private val threadLocal = new ThreadLocal[SparkContext]()

    def putEnv(master:String)(implicit appName:String) = {
        val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
        val sc = new SparkContext(sparkConf)
        threadLocal.set(sc)
        sc
    }
    def getEnv() = {
        threadLocal.get()
    }
    def removeEnv(): Unit = {
        threadLocal.remove()
    }
    def closeEnv(): Unit = {
        getEnv().stop()
        removeEnv()
    }

}
