package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql._

object SparkSQL09_Req_1 {

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
              |   *
              |from (
              |    select
              |       *,
              |       rank() over ( partition by area order by clickCnt desc ) as rank
              |    from (
              |        select
              |           area,
              |           product_name,
              |           count(*) as clickCnt
              |        from (
              |            select
              |               a.*,
              |               c.area,
              |               c.city_name,
              |               p.product_name
              |            from user_visit_action a
              |            join city_info c on a.city_id = c.city_id
              |            join product_info p on a.click_product_id = p.product_id
              |            where a.click_product_id > -1
              |        ) t1 group by area, product_name
              |    ) t2
              |) t3 where rank <= 3
            """.stripMargin).show()


        // 关闭环境
        spark.stop()

    }
}
