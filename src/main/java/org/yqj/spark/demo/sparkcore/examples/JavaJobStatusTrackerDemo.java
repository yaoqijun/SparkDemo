package org.yqj.spark.demo.sparkcore.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yaoqijun.
 * Date:2016-06-27
 * Email:yaoqj@terminus.io
 * Descirbe: spark job 执行信息跟踪
 */
public class JavaJobStatusTrackerDemo {

    public static void main(String[] args) throws Exception{
        SparkConf sparkConf = new SparkConf().setAppName("sparkStatusApi");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // rdd
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(f->{
            //  Job 暂停执行时间
            Thread.sleep( 2 * 1000 );
            return f;
        });

        // rdd async action
        JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
        while (!jobFuture.isDone()){
            System.out.println("********* job not done *********");
            Thread.sleep(1000);  // 1 second

            List<Integer> jobIds = jobFuture.jobIds();
            if (jobIds.isEmpty()) {
                continue;
            }
            int currentJobId = jobIds.get(jobIds.size() - 1);
            SparkJobInfo jobInfo = javaSparkContext.statusTracker().getJobInfo(currentJobId);
            SparkStageInfo stageInfo = javaSparkContext.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
            System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() +
                    " active, " + stageInfo.numCompletedTasks() + " complete");
        }

    }
}
