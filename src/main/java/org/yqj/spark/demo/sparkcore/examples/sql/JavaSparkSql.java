package org.yqj.spark.demo.sparkcore.examples.sql;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by yaoqijun.
 * Date:2016-06-28
 * Email:yaoqj@terminus.io
 * Descirbe: spark sql context 测试
 */
public class JavaSparkSql {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("javaSparkName");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        // read data
        System.out.println("*********** spark read data from text context **************");
        JavaRDD<Person> personJavaRDD = javaSparkContext.textFile("persons.txt").map(s->{
            String[] personStr = s.split(",");
            Person person = new Person();
            person.setName(personStr[0]);
            person.setAge(Integer.valueOf(personStr[1].trim()));
            return person;
        });

        System.out.println("*********** data frame builder table content *********");
        DataFrame dataFrame = sqlContext.createDataFrame(personJavaRDD, Person.class);
        dataFrame.registerTempTable("person");

        System.out.println("*********** execute sql content from person **********");
        DataFrame teenagers = sqlContext.sql("select name from person where age >= 13 and age <= 19");
        teenagers.toJavaRDD().map(row->"Name is :"+row.getString(0)).collect().forEach(System.out::println);

        System.out.println("****** 输出对应的 parquet 文件内容  ********");
        dataFrame.write().parquet("person.parquet");

        javaSparkContext.stop();
    }

    @Data
    public static class Person implements Serializable{

        private static final long serialVersionUID = 6506978660566683955L;

        private String name;

        private Integer age;

    }

}
