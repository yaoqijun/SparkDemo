package org.yqj.spark.demo.sparkcore.examples.steaming;

import com.google.common.io.Closeables;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Arrays;

/**
 * Created by yaoqijun.
 * Date:2016-06-28
 * Email:yaoqj@terminus.io
 * Descirbe:  用户自定义， 数据的接收， 不是 Spark 定时的数据获取， \n  socket 数据获取 时候， 回掉对应的数据接口
 */
public class JavaCustomReceiver extends Receiver<String>{

    String host = "localhost";
    Integer port = 9999;

    public JavaCustomReceiver(String host, Integer port) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));

        JavaReceiverInputDStream<String> lines = ssc.receiverStream(
                new JavaCustomReceiver("localhost", 9999));

        lines.flatMap(s->{
            System.out.println("read from memory is s:{} " + s);
            return Arrays.asList(s.split(" "));
        }).print();

        ssc.start();
        ssc.awaitTermination();
    }

    @Override
    public StorageLevel storageLevel() {
        return null;
    }

    @Override
    public void onStart() {
        new Thread()  {
            @Override public void run() {
                receive();
            }
        }.start();
    }

    @Override
    public void onStop() {

    }

    private void receive(){
        try {
            Socket socket = null;
            BufferedReader reader = null;
            String userInput = null;
            try {
                socket = new Socket(host, port);
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                while (!isStopped() && (userInput = reader.readLine()) != null) {
                    System.out.println("Received data '" + userInput + "'");

                    //将 接受的数据 推入 对应的Spark Memory 中
                    store(userInput);
                }
            } finally {
                Closeables.close(reader, true);
                Closeables.close(socket, true);
            }
            restart("*********** Trying to connect again");
        } catch(ConnectException ce) {
            restart("Could not connect", ce);
        } catch(Throwable t) {
            restart("Error receiving data", t);
        }
    }
}
