package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.common.CommonService
import com.atguigu.bigdata.spark.core.framework.dao.HotCategoryTop10Dao
import org.apache.spark.rdd.RDD

/**
  * 热门品类Top10服务对象
  */
class HotCategoryTop10Service_2 extends CommonService {

    private val hotCategoryTop10Dao = new HotCategoryTop10Dao

    override def analysis() = {

        // TODO 1. 获取用户行为数据
        val datas: RDD[String] = hotCategoryTop10Dao.getFileData("input/user_visit_action.txt")

        // line => (品类，（1，0，0）)
        // line => (品类1，（0，1，0）)(品类2，（0，1，0）)
        // line => (品类1，（0，0，1）)(品类2，（0，0，1）)
        val mapDatas = datas.flatMap(
            line => {
                val dats = line.split("_")
                if ( dats(6) != "-1" ) {
                    // 点击数据
                    List( (dats(6), (1,0,0)) )
                } else if ( dats(8) != "null" ) {
                    // 下单数据
                    val ids = dats(8).split(",")
                    ids.map((_, (0, 1, 0)))
                } else if ( dats(10) != "null" ) {
                    // 支付数据
                    val ids = dats(10).split(",")
                    ids.map((_, (0, 0, 1)))
                } else {
                    Nil.iterator
                }
            }
        )

        val categoryData = mapDatas.reduceByKey(
            ( t1, t2 ) => {
                ( t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )

        categoryData.sortBy(_._2, false).take(10)

    }
}
