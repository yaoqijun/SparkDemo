package org.yqj.spark.demo.sparkcore.examples.steaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by yaoqijun.
 * Date:2016-06-28
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class JavaQueueSteam {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaQueueStream");

        // Create the context
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        // Create the queue through which RDDs can be pushed to
        // a QueueInputDStream
        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<JavaRDD<Integer>>();

        // Create and push some RDDs into the queue
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }

        for (int i = 0; i < 30; i++) {
            rddQueue.add(ssc.sparkContext().parallelize(list));
        }

        // Create the QueueInputDStream and use it do some processing
        JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
        JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(
                new PairFunction<Integer, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Integer i) {
                        return new Tuple2<Integer, Integer>(i % 10, 1);
                    }
                });
        JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        reducedStream.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
