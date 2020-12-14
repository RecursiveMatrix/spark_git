package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql.{Encoder, _}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkSQL09_Req_2 {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "root")
        // TODO SparkSQL
        // 创建环境
        val spark : SparkSession =
            SparkSession.builder()
                .enableHiveSupport() // 启用Hive的支持
                .master("local[*]").appName("SparkSQL").getOrCreate()

        spark.sql("use atguigu200820")

        spark.sql(
            """
              |select
              |   a.*,
              |   c.area,
              |   c.city_name,
              |   p.product_name
              |from user_visit_action a
              |join city_info c on a.city_id = c.city_id
              |join product_info p on a.click_product_id = p.product_id
              |where a.click_product_id > -1
            """.stripMargin).createOrReplaceTempView("t1")

        // 创建聚合函数
        val cityRemarkUDAF = new CityRemarkUDAF
        spark.udf.register("cityRemark", functions.udaf(cityRemarkUDAF))

        spark.sql(
            """
              |select
              |   area,
              |   product_name,
              |   count(*) as clickCnt,
              |   cityRemark(city_name) as cityremark
              |from t1 group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")

        spark.sql(
            """
              |    select
              |       *,
              |       rank() over ( partition by area order by clickCnt desc ) as rank
              |    from t2
            """.stripMargin).createOrReplaceTempView("t3")

        spark.sql(
            """
              |select
              |   *
              |from t3 where rank <= 3
            """.stripMargin).show(false)



        // 关闭环境
        spark.stop()

    }
    // Map[（北京，100), (上海，200)]
    case class CityBuff( var total : Long, var cityMap:mutable.Map[String, Long] )
    // 自定义城市备注聚合函数
    // 1. 继承Aggregator
    // 2. 定义泛型
    //    IN : 城市名称
    //    BUF: CityBuff[城市点击总和，Map[每个城市点击数量]]
    //    OUT : 备注
    // 3. 重写方法
    class CityRemarkUDAF extends Aggregator[String, CityBuff, String]{
        override def zero: CityBuff = {
            CityBuff( 0L, mutable.Map[String, Long]() )
        }

        override def reduce(b: CityBuff, city: String): CityBuff = {
            b.total += 1
            val newCount = b.cityMap.getOrElse(city, 0L) + 1
            b.cityMap.update(city, newCount)
            b
        }

        override def merge(b1: CityBuff, b2: CityBuff): CityBuff = {
            b1.total += b2.total

            val map1 = b1.cityMap
            val map2 = b2.cityMap

            map2.foreach {
                case ( city, cnt ) => {
                    val newCount = map1.getOrElse(city, 0L) + cnt
                    map1.update(city, newCount)
                }
            }

            b1.cityMap = map1
            b1
        }

        override def finish(buff: CityBuff): String = {
            val ss = ListBuffer[String]()

            val totalCnt = buff.total
            val cityMap: mutable.Map[String, Long] = buff.cityMap

            // 城市点击需要排序
            val sortList: List[(String, Long)] = cityMap.toList.sortWith(
                (t1, t2) => {
                    t1._2 > t2._2
                }
            ).take(2)

            var hasMore = cityMap.size > 2

            var sum = 100L

            sortList.foreach{
                case ( city, cnt ) => {
                    // cnt : 100
                    val r = cnt * 100 / totalCnt
                    ss.append(s"${city} ${r}%")
                    sum = sum - r
                }
            }

            if (hasMore) {
                ss.append(s"其他 ${sum}%")
            }

            ss.mkString(", ")
        }

        override def bufferEncoder: Encoder[CityBuff] = Encoders.product
        override def outputEncoder: Encoder[String] = Encoders.STRING
    }
}
