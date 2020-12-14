package com.atguigu.bigdata.spark.core.framework.helper

import com.atguigu.bigdata.spark.core.framework.bean.HotCategoryAnalysis
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 热门品类累加器
  * Map[（品类1,(click, order, pay)）,（品类2,(click, order, pay)）]
  * 1. 继承AccumulatorV2
  * 2. 定义泛型
  *    IN :  (品类，用户行为)
  *    OUT:  Map[ 品类， HotCategoryAnalysis ]
  * 3. 重新方法
  */
class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String,HotCategoryAnalysis]]{

    private var hcmap = mutable.Map[String, HotCategoryAnalysis]()

    override def isZero: Boolean = {
        hcmap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategoryAnalysis]] = {
        new HotCategoryAccumulator
    }

    override def reset(): Unit = {
        hcmap.clear()
    }

    override def add(v: (String, String)): Unit = {
        val cid = v._1
        val actionType = v._2

        val analysis: HotCategoryAnalysis = hcmap.getOrElse(cid, HotCategoryAnalysis(cid, 0, 0, 0))
        actionType match {
            case "click" => analysis.clickCnt += 1
            case "order" => analysis.orderCnt += 1
            case "pay" => analysis.payCnt += 1
        }
        // updated 方法用于产生新的map集合
        // update方法用于更新当前集合，不会产生新的
        hcmap.update(cid, analysis)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategoryAnalysis]]): Unit = {

        val map1 = hcmap
        val map2 = other.value

        hcmap = map1.foldLeft(map2) {
            ( m, kv ) => {
                val cid = kv._1
                val hc  = kv._2
                //[品类1， （100， 200， 300）]
                //[品类1， （150， 250， 340）]
                // [品类1， （250， 450， 640）]
                val analysis: HotCategoryAnalysis = m.getOrElse(cid, HotCategoryAnalysis(cid, 0, 0, 0))
                analysis.clickCnt += hc.clickCnt
                analysis.orderCnt += hc.orderCnt
                analysis.payCnt   += hc.payCnt

                m.update(cid, analysis)
                m
            }
        }

    }

    override def value: mutable.Map[String, HotCategoryAnalysis] = hcmap
}
