package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.common.CommonService
import com.atguigu.bigdata.spark.core.framework.dao.HotCategoryTop10Dao
import org.apache.spark.rdd.RDD

/**
  * 热门品类Top10服务对象
  */
class HotCategoryTop10Service_1 extends CommonService {

    private val hotCategoryTop10Dao = new HotCategoryTop10Dao

    override def analysis() = {

        // TODO 1. 获取用户行为数据
        val datas: RDD[String] = hotCategoryTop10Dao.getFileData("input/user_visit_action.txt")

        // 将RDD的数据缓存起来，用于重复读取
        datas.cache()

        // TODO 2. 从用户行为数据中筛选点击数据
        //         将筛选后的数据进行统计 => （品类， 点击数量）
        val clickData = datas.filter(
            line => {
                val dats = line.split("_")
                dats(6) != "-1"
            }
        ).map(
            line => {
                // (品类1，1)，(品类1，1)
                val dats = line.split("_")
                (dats(6), 1)
            }
        ).reduceByKey(_+_)


        // TODO 3. 从用户行为数据中筛选下单数据
        //         将筛选后的数据进行统计 => （品类， 下单数量）
        val orderData = datas.filter(
            line => {
                val dats = line.split("_")
                dats(8) != "null"
            }
        ).flatMap(
            line => {
                val dats = line.split("_")
                // (品类1,品类2.品类3)
                val cs = dats(8).split(",")
                //(品类1,1),(品类2,1).(品类3,1)
                cs.map((_,1))
            }
        ).reduceByKey(_+_)

        // TODO 4. 从用户行为数据中筛选支付数据
        //         将筛选后的数据进行统计 => （品类， 支付数量）
        val payData = datas.filter(
            line => {
                val dats = line.split("_")
                dats(10) != "null"
            }
        ).flatMap(
            line => {
                val dats = line.split("_")
                // (品类1,品类2.品类3)
                val cs = dats(10).split(",")
                //(品类1,1),(品类2,1).(品类3,1)
                cs.map((_,1))
            }
        ).reduceByKey(_+_)

        // TODO 5. 对点击数量和下单数量，支付数量进行排序（降序），取前10名
        //         Tuple(元组)
        // （品类，点击数量）=> ( 品类，（点击数量，0，0） )
        val clickMapData = clickData.map{
            case ( cid, clickCnt ) => {
                ( cid, (clickCnt, 0, 0) )
            }
        }
        // （品类，下单数量）=> ( 品类，（0，下单数量，0） )
        val orderMapData = orderData.map{
            case ( cid, orderCnt ) => {
                ( cid, (0, orderCnt, 0) )
            }
        }
        // （品类，支付数量）=> ( 品类，（0，0，支付数量） )
        val payMapData = payData.map{
            case ( cid, payCnt ) => {
                ( cid, (0, 0, payCnt) )
            }
        }
        // =>
        // (品类，（点击数量，下单数量，支付数量）)
        // TODO 将不同的统计结果按照品类的ID汇总在一起
        val mapDatas = clickMapData.union(orderMapData).union(payMapData)

        val categoryData = mapDatas.reduceByKey(
            ( t1, t2 ) => {
                ( t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )

        categoryData.sortBy(_._2, false).take(10)

    }
}
