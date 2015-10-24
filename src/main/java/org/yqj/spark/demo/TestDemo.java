package org.yqj.spark.demo;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by yaoqijun.
 * Date:2015-10-24
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class TestDemo {
    public static void main(String []args){
        System.out.println("test java8");
        Function<String,Integer> function = (t) -> Integer.valueOf(t);
        System.out.println(function.apply("123"));

        Function<String,Long> testHaha = (t) -> Long.valueOf(t);
        System.out.println(testHaha.apply("123123"));

        Predicate<Integer> predicate = (pam) -> {
            if(pam > 0){
                return true;
            }else {
                return false;
            }
        };
        System.out.println(predicate.test(100));
        System.out.println(predicate.test(-100));
    }
}
