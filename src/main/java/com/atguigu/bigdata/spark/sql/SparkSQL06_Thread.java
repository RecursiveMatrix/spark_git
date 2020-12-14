package com.atguigu.bigdata.spark.sql;

public class SparkSQL06_Thread {
    public static void main(String[] args) {

        // 线程启动 start
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                //return
                // throw
                //i++; 不是原子性
            }
        });
        t.start();
        // 只要跳出线程的run方法，线程就中止了

        // 线程停止 ： stop
        t.stop();

    }
}
