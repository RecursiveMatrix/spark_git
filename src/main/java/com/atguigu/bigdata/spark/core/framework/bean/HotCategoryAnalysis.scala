package com.atguigu.bigdata.spark.core.framework.bean

case class HotCategoryAnalysis(
   categoryid:String,
   var clickCnt : Int,
   var orderCnt : Int,
   var payCnt : Int
)
