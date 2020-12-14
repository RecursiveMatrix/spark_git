package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.bean.{HotCategoryAnalysis, UserVisitAction}
import com.atguigu.bigdata.spark.core.framework.common.CommonService
import com.atguigu.bigdata.spark.core.framework.dao.HotCategorySessionTop10Dao
import org.apache.spark.rdd.RDD

/**
  * 热门品类Top10服务对象
  */
class HotCategorySessionTop10Service_1 extends CommonService {

    private val hotCategorySessionTop10Dao = new HotCategorySessionTop10Dao

    override def analysis(data:Any) = {
        // 需求一的结果
        val top10: List[HotCategoryAnalysis] = data.asInstanceOf[List[HotCategoryAnalysis]]
        val top10Id: List[String] = top10.map(_.categoryid)

        // TODO 1. 获取用户行为数据
        val datas: RDD[String] = hotCategorySessionTop10Dao.getFileData("input/user_visit_action.txt")

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
        // TODO 2. 将数据进行筛选过滤
        val filterDatas = actionDatas.filter(
            data => {
                if ( data.click_category_id.toString != "-1" ) {
                    top10Id.contains(data.click_category_id.toString)
                } else {
                    false
                }
            }
        )

        // TODO 3. 将数据转换结构
        // （品类-会话，1）
        // TODO 4. 将转换结构后的数据进行统计
        // （品类-会话，1） -> （品类-会话，sum）
        val wordCount = filterDatas.map(
            data => {
                (data.click_category_id + "-" + data.session_id, 1)
            }
        ).reduceByKey(_+_)

        // TODO 5. 将统计的结果转换结构
        // （品类-会话，sum）-> (品类，（会话，sum）)
        val mapData = wordCount.map {
            case ( k, sum ) => {
                val ks = k.split("-")
                ( ks(0), (ks(1), sum) )
            }
        }

        // TODO 6. 将转换结构后的数据根据品类进行分组
        //  (品类，（会话1，sum）)  (品类，（会话2，sum）)
        // Map[品类，Iterator[ （会话1，sum）, （会话2，sum） ]]
        val groupData: RDD[(String, Iterable[(String, Int)])] = mapData.groupByKey()

        // TODO 7. 将分组后的数据根据点击数量进行排序（降序），取前10名
        groupData.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        ).collect

    }
}
