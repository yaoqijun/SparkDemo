package org.yqj.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by yaoqijun.
 * Date:2015-10-24
 * Email:yaoqj@terminus.io
 * Descirbe: 数据流中获取数据内容进行统计,不是连续进行的。
 */
public class JavaNetworkWordCount {

    private static final Pattern BLANK = Pattern.compile(" ");

    public static void main(String []args){
        //Streaming log content

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");   //通过conf或Context 创建对应的Stream
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // create socket with ip port content
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost",
                Integer.valueOf("9999"), StorageLevels.MEMORY_AND_DISK_SER);

        //split content by " "
        JavaDStream<String> words = lines.flatMap((s) -> Arrays.asList(BLANK.split(s)));

        //count word number by lambda   java8 diao
        JavaPairDStream<String,Integer> wordCounts = words.mapToPair(new PairFunction<String,String,Integer>(){
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        }).reduceByKey((n1,n2)->n1+n2);

        //输出对应的结果
        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
