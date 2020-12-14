package com.atguigu.bigdata.spark.core.rdd.oper;
import java.util.*;
public class Spark03_Oper_Java {
    public static void main(String[] args) {

        User user = new User();
        user.name = "zhangsan";

        ArrayList<User> users = new ArrayList<User>();
        users.add(user);

        // Java Clone克隆浅复制
        final ArrayList<User> users1 = (ArrayList<User>)users.clone();
        User otherUser = users1.get(0);
        otherUser.name = "lisi";

        //System.out.println(users == users1); // false
        System.out.println(user.name);
        System.out.println(otherUser.name);


    }
}
class User {
    public String name;
}
