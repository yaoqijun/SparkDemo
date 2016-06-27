package org.yqj.spark.demo.sparkcore.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yaoqijun.
 * Date:2016-06-27
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class JavaWordCount {
    public static void main(String[] args) {

        System.out.println("******* start spark count wor test ******");

        // init context
        SparkConf sparkConf = new SparkConf().setAppName("javaWordCount");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // count word
        List<Tuple2<String, Integer>> result = javaSparkContext.textFile("wordCount.txt")
                .flatMap(s -> Arrays.asList(s.split(" ")))
                .mapToPair(s -> new Tuple2<String, Integer>(s, 1))
                .reduceByKey((a, b)->a+b).collect();

        // out result
        System.out.println("********** result word count **********");
        result.forEach(s->{
            System.out.println("word: "+s._1() + ", count: "+s._2());
        });
    }
}
