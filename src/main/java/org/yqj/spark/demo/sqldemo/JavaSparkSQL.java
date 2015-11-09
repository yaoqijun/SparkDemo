package org.yqj.spark.demo.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.List;

/**
 * Created by yaoqijun.
 * Date:2015-11-09
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class JavaSparkSQL {
    public static void main(String[] args) throws Exception{
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        System.out.println("=== Data source: RDD ===");
        // Load a text file and convert each line to a Java Bean.
        JavaRDD<Person> people = javaSparkContext.textFile("people.txt").map((line) -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setName(parts[0]);
            person.setAge(parts[1]);
            return person;
        });

        // Apply a schema to an RDD of Java Beans and register it as a table.
        DataFrame dataFrame = sqlContext.createDataFrame(people, Person.class);
        dataFrame.registerTempTable("people");

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        //通过DataFrame 获取对应的RDD collection 获取对应的结果内容
        List<String> teenagersName = teenagers.toJavaRDD().map((row) -> "Name : " + row.getString(0)).collect();

        teenagersName.forEach((name)->{
            System.out.println(name);
        });
    }

    public static class Person implements Serializable{
        private String name;
        private String age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }
    }
}
