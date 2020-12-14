package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.bean.HotCategoryAnalysis
import com.atguigu.bigdata.spark.core.framework.common.CommonService
import com.atguigu.bigdata.spark.core.framework.dao.HotCategoryTop10Dao
import com.atguigu.bigdata.spark.core.framework.helper.HotCategoryAccumulator
import com.atguigu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 热门品类Top10服务对象
  */
class HotCategoryTop10Service_3 extends CommonService {

    private val hotCategoryTop10Dao = new HotCategoryTop10Dao

    override def analysis() = {

        // TODO 1. 获取用户行为数据
        val datas: RDD[String] = hotCategoryTop10Dao.getFileData("input/user_visit_action.txt")

        // 创建累加器
        val acc = new HotCategoryAccumulator
        // 注册累加器
        EnvUtil.getEnv().register(acc, "HotCategory")

        datas.foreach(
            line => {
                val dats = line.split("_")
                if ( dats(6) != "-1" ) {
                    // 点击数据
                    acc.add( (dats(6), "click") )
                } else if ( dats(8) != "null" ) {
                    // 下单数据
                    val ids = dats(8).split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "order") )
                        }
                    )
                } else if ( dats(10) != "null" ) {
                    // 支付数据
                    val ids = dats(10).split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "pay") )
                        }
                    )
                }
            }
        )

        // 获取累加器的结果
        val accMap: mutable.Map[String, HotCategoryAnalysis] = acc.value
        val accIter: mutable.Iterable[HotCategoryAnalysis] = accMap.map(_._2)

        accIter.toList.sortWith(
            (hc1, hc2) => {
                if ( hc1.clickCnt > hc2.clickCnt ) {
                    true
                } else if (hc1.clickCnt == hc2.clickCnt) {
                    if( hc1.orderCnt > hc2.orderCnt ) {
                        true
                    } else if ( hc1.orderCnt == hc2.orderCnt ) {
                        hc1.payCnt > hc2.payCnt
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        ).take(10)


    }
}
