package org.yqj.spark.demo.sparkcore.examples.steaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by yaoqijun.
 * Date:2016-06-27
 * Email:yaoqj@terminus.io
 * Descirbe: java steam 通过 网络定时数量统计
 */
public class JavaNetworkWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("sparkNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines.flatMap(x-> {
            System.out.print("******** get string x :" + x);
            return Arrays.asList(x.split(" "));
        });
        JavaPairDStream<String, Integer> wordCounts =
                words.mapToPair(x->new Tuple2<String, Integer>(x,1))
                        .reduceByKey((a,b)->a+b);
        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
