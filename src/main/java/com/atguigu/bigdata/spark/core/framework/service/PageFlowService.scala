package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.bean.{HotCategoryAnalysis, UserVisitAction}
import com.atguigu.bigdata.spark.core.framework.common.CommonService
import com.atguigu.bigdata.spark.core.framework.dao.{HotCategorySessionTop10Dao, PageFlowDao}
import org.apache.spark.rdd.RDD

/**
  * 热门品类Top10服务对象
  */
class PageFlowService extends CommonService {

    private val pageFlowDao = new PageFlowDao

    override def analysis() = {
        val datas: RDD[String] = pageFlowDao.getFileData("input/user_visit_action.txt")

        val actionDatas = datas.map(
            line => {
                val dats = line.split("_")
                UserVisitAction(
                    dats(0),
                    dats(1).toLong,
                    dats(2),
                    dats(3).toLong,
                    dats(4),
                    dats(5),
                    dats(6).toLong,
                    dats(7).toLong,
                    dats(8),
                    dats(9),
                    dats(10),
                    dats(11),
                    dats(12).toLong
                )
            }
        )

        actionDatas.cache()

        // TODO 计算分母 (页面ID, sum)
        val pageClickDatas = actionDatas.map(
            action => {
                (action.page_id,1)
            }
        ).reduceByKey(_+_).collect().toMap

        // TODO 计算分子
        // TODO 1. 根据用户Session对象数据分组
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionDatas.groupBy(_.session_id)

        // TODO 2. 对分组后的数据根据访问时间进行排序
        val mapRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                val actions: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

                // 1,2,3,5,7,9
                // 2,3,5,7,9
                val pageids: List[Long] = actions.map(_.page_id)

                // 页面的单跳
                // 1-2，2-3.3-5，5-7，7-9
                // (1, 2), (2,3)
                pageids.zip(pageids.tail).map {
                    case (pageid1, pageid2) => {
                        (pageid1 + "-" + pageid2, 1)
                    }
                }
            }
        )
        // (1-2,1)
        // List((1-2,1))
        val pageflowRDD: RDD[(String, Int)] = mapRDD.map(_._2).flatMap(list=>list)

        // 1-2, 100 / 1
        val pageidSumClick: RDD[(String, Int)] = pageflowRDD.reduceByKey(_+_)
        pageidSumClick.foreach{
            case ( pageids, sum ) => {

                val pids = pageids.split("-")
                // 分子
                val a = sum
                // 分母
                val b = pageClickDatas.getOrElse(pids(0).toLong, 1)

                println(s"页面【${pageids}】单跳转换率为 = " + a.toDouble/b)
            }
        }

    }
}
