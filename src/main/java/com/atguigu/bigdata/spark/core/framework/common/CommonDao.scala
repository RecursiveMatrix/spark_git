package com.atguigu.bigdata.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

trait CommonDao {

    def getFileData(path:String) = {
        val lines: RDD[String] = EnvUtil.getEnv().textFile(path)
        lines
    }
    def getMemoryData() = {
        EnvUtil.getEnv().makeRDD(List(1,2,3,4))
    }
}
