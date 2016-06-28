package org.yqj.spark.demo.sparkcore.examples.steaming;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by yaoqijun.
 * Date:2016-06-28
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class JavaStatefulNetworkWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaStatefulNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        ssc.checkpoint(".");

        // Initial state RDD input to mapWithState
        List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<String, Integer>("hello", 1),
                new Tuple2<String, Integer>("world", 1));
        JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x));
            }
        });

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

//        // DStream made of get cumulative counts that get updated in every batch
//        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
//                wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));
//
//        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();
    }

}
