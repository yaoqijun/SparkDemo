package org.yqj.spark.demo.sparkcore.practise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by yaoqijun.
 * Date:2016-04-03
 * Email:yaoqj@terminus.io
 * Descirbe: 测试对应的Context 信息
 */
public class ContextTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
    }
}
