package org.yqj.spark.demo.sparkcore.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yaoqijun.
 * Date:2016-06-27
 * Email:yaoqj@terminus.io
 * Descirbe: 通过 概率 方式 求 PI 数值
 */
public class JavaSparkToPI {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("sparkPiValue");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        int slices = 2;
        int n = 200000;

        List<Integer> l = new ArrayList<>(n);
        for (int i=0; i<n; i++){
            l.add(i);
        }

        JavaRDD<Integer> dataSet = javaSparkContext.parallelize(l, slices);

        int count = dataSet.map(a->{
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0;
        }).reduce((a,b)->a+b);

        System.out.println("************ Pi is roughly " + 4.0 * count / n);

        javaSparkContext.stop();
    }
}
