package com.atguigu.bigdata.spark.core.rdd.oper;

import org.apache.commons.math3.random.RandomAdaptor;

import java.util.Random;

public class Spark09_Oper_Java {
    public static void main(String[] args) {

        Random r = new Random(10);
        for ( int i = 0; i < 5; i++ ) {
            System.out.println(r.nextInt(10));
        }
        System.out.println("*******************");
        Random r1 = new Random(10);
        for ( int i = 0; i < 5; i++ ) {
            System.out.println(r1.nextInt(10));
        }
    }
}
