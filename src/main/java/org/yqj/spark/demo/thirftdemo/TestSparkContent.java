package org.yqj.spark.demo.thirftdemo;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by yaoqijun.
 * Date:2015-10-29
 * Email:yaoqj@terminus.iove
 * Descirbe:
 */
public class TestSparkContent {

    public static void main(String []args){
        System.out.println("test hive content");
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("terminus-spark-context.xml");
//        HiveTemplate hiveTemplate = context.getBean(HiveTemplate.class);
        JdbcTemplate template = context.getBean(JdbcTemplate.class);
//        List<Map<String,Object>> result = template.queryForList("select * from user_test");
//        System.out.println(result.toString());

        String sql = "";
        //test get database info, get tables info
        //List<String> result = hiveTemplate.query("show tables");
//        List<Map<String,Object>> result = template.queryForList("show tables");
//        System.out.println(result.toString());
        //List<Map<String,Object>> result = template.queryForList("show databases");
        //System.out.println(result.toString());

        //test table execute
        //sql = " create table test_create(id int, name string, age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ':'";
        //template.execute(sql);
//        sql = "drop table test_create";
//        template.execute(sql);

        //hiveOperations.executeScript(new HiveScript());
//        if(hiveOperations == null){
//            System.out.println("empty");
//            sql = "load data inpath '/user/yaoqijun/part-m-00000' into table test_create";
//            hiveOperations.query(sql);
//        }
        //load data
        //sql = "load data inpath '/user/yaoqijun/part-m-00000' into table test_create";
        //template.execute(sql);

        //select data
//        sql = "SELECT * from parana_addresses";
//        List<Map<String,Object>> result2 = template.queryForList(sql);
//        System.out.println(result2.toString());

//        sql = "insert into table parana_addresses_again VALUES(1,1,'test',1,1)";
//        sql = "INSERT INTO TABLE parana_addresses_again SELECT * from parana_addresses";
//        sql = "select * FROM parana_users";
//        template.execute(sql);

        testSelect(template);
    }

    public static void testSelect(JdbcTemplate template){
        String sql = "select id from parana_users where type =2 and status=1";
        sql = "select d.id, e.nickname,d.total,d.count from " +
                "(select id,sum(fee) total,count(fee) count from " +
                "(select a.id, b.fee from " +
                "(select id from parana_users where type =1 and status=1) as a " +
                "left join parana_orders b on a.id = b.buyer_id where b.status=3) as c " +
                "group by id) as d " +
                "left join parana_users e on d.id = e.id";
        sql = "select sum(count) count,sum(amount) amount from `pigmall_report_sum_of_dailys` where summed_at = '2015-09-27'";
        sql = "select sum(count) count, sum(amount) amount,summed_at from `pigmall_report_sum_of_dailys` group by summed_at";

        List<Map<String,Object>> result2 = template.queryForList(sql);
        System.out.println(result2.toString());
    }
}
