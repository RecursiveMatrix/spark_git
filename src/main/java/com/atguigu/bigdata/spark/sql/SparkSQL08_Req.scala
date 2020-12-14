package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql._

object SparkSQL08_Req {

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
              |CREATE TABLE `user_visit_action`(
              |  `date` string,
              |  `user_id` bigint,
              |  `session_id` string,
              |  `page_id` bigint,
              |  `action_time` string,
              |  `search_keyword` string,
              |  `click_category_id` bigint,
              |  `click_product_id` bigint,
              |  `order_category_ids` string,
              |  `order_product_ids` string,
              |  `pay_category_ids` string,
              |  `pay_product_ids` string,
              |  `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)
        spark.sql(
            """
              |load data local inpath 'input/user_visit_action.txt' into table atguigu200820.user_visit_action
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `product_info`(
              |  `product_id` bigint,
              |  `product_name` string,
              |  `extend_info` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'input/product_info.txt' into table atguigu200820.product_info
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `city_info`(
              |  `city_id` bigint,
              |  `city_name` string,
              |  `area` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'input/city_info.txt' into table atguigu200820.city_info
            """.stripMargin)

        spark.sql(
            """
              |select * from atguigu200820.city_info
            """.stripMargin).show

        // 关闭环境
        spark.stop()

    }
}
