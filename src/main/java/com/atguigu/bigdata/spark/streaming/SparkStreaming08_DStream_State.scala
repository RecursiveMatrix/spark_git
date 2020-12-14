package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_DStream_State {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming
        // 从检查点恢复数据
        val sparkStreamingContext = StreamingContext.getActiveOrCreate("cp", ()=>{
            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
            val ssc = new StreamingContext(sparkConf, Seconds(5))
            ssc.checkpoint("cp")

            val ds = ssc.socketTextStream("localhost", 7777)

            val wordDS = ds.flatMap(_.split(" "))
            val wordToOneDS = wordDS.map((_,1))

            //wordToOneDS.reduceByKey(_+_) // 所谓的无状态操作，就是计算时不考虑缓冲
            // 有状态数据操作
            // updateStateByKey传递的参数是一个函数
            // 这个函数有2个参数
            //     第一个参数表示相同key的value的集合（Seq）
            //     第二个参数表示缓冲区数据对象

            // The checkpoint directory has not been set.
            // 有状态计算其实就是使用缓冲区进行计算，这个缓冲区其实采用的是检查点操作
            val stateDS = wordToOneDS.updateStateByKey{
                ( seq:Seq[Int], buffer:Option[Int] ) => {
                    val currentVal = seq.sum
                    val buffVal = buffer.getOrElse(0)
                    val newVal = currentVal + buffVal
                    Option(newVal)
                }
            }

            stateDS.print()
            ssc
        })


        sparkStreamingContext.start()
        sparkStreamingContext.awaitTermination()
    }
}
