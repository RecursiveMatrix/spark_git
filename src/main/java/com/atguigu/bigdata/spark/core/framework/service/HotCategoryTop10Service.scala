package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.common.CommonService
import com.atguigu.bigdata.spark.core.framework.dao.HotCategoryTop10Dao
import org.apache.spark.rdd.RDD

/**
  * 热门品类Top10服务对象
  */
class HotCategoryTop10Service extends CommonService {

    private val hotCategoryTop10Dao = new HotCategoryTop10Dao

    override def analysis() = {

        // TODO 1. 获取用户行为数据
        val datas: RDD[String] = hotCategoryTop10Dao.getFileData("input/user_visit_action.txt")

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
        // （品类，点击数量）
        // （品类，下单数量）
        // （品类，支付数量）
        // =>
        // (品类，（点击数量，下单数量，支付数量）)
        // TODO 将不同的统计结果按照品类的ID汇总在一起
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
            clickData.cogroup(orderData, payData)

        val categoryData = cogroupRDD.map{
            case ( category, (clickIter, orderIter, payIter) ) => {
                var clickCnt = 0
                val clickIterator: Iterator[Int] = clickIter.iterator
                if (clickIterator.hasNext) {
                    clickCnt = clickIterator.next()
                }

                var orderCnt = 0
                val orderIterator: Iterator[Int] = orderIter.iterator
                if (orderIterator.hasNext) {
                    orderCnt = orderIterator.next()
                }

                var payCnt = 0
                val payIterator: Iterator[Int] = payIter.iterator
                if (payIterator.hasNext) {
                    payCnt = payIterator.next()
                }

                (category, (clickCnt, orderCnt, payCnt))
            }
        }

        categoryData.sortBy(_._2, false).take(10)

    }
}
