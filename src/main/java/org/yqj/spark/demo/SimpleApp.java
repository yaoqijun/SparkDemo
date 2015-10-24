package org.yqj.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by yaoqijun.
 * Date:2015-10-24
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class SimpleApp {
    public static void main(String []args){
        System.out.println("test spark");
        String logFile = "testFile.md";
        SparkConf sparkConf = new SparkConf().setSparkHome("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        Long lineHasA = logData.filter((s) -> s.contains("a")).count();

        Long lineHasB = logData.filter((s) -> s.contains("b")).count();

        System.out.println("line contain a is "+ lineHasA +"line contain b is " + lineHasB);

        //output every line content
        logData.foreach(s -> System.out.println(s));
    }
}
