package org.yqj.spark.demo.sparkcore.examples.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by yaoqijun.
 * Date:2016-07-05
 * Email:yaoqj@terminus.io
 * Descirbe: 测试配置hive Context 内容选择方式
 */
public class JavaHiveSparkContextSql {

    public static void main(String[] args) {
        System.out.println("*********** test java hive spark context sql ***********");
        SparkConf sparkConf = new SparkConf().setAppName("hiveSqlContextTest");
//        sparkConf.setMaster("spark://yaoqijuns-MacBook-Pro.local:7077");

        SparkContext sparkContext = new SparkContext(sparkConf);
        HiveContext hiveContext = new HiveContext(sparkContext);

        // 测试 hive 数据内容
        Row[] rows = hiveContext.sql("select * from test").collect();
        System.out.println("************ rows result content **********");
        for (Row row : rows){
            System.out.println(row.toString());
        }
    }

}
