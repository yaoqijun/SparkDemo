package org.yqj.spark.demo.thirftdemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * Created by yaoqijun.
 * Date:2015-10-30
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class TestMain {

        public static void main(String[] args) throws Exception{
            String result = "";
            Connection conn = null;
            Statement stmt = null;
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection(
                    "jdbc:hive2://yaoqijuns-MacBook-Pro.local:14000/default", "yaoqijun", "yao4094230");
            stmt = conn.createStatement();
            String sql = "select * from user_test";
            long start = System.currentTimeMillis();

            ResultSet res = stmt.executeQuery(sql);

            ResultSetMetaData rsmd = res.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // 输出列名
            for (int i=1; i<=columnCount; i++){
                System.out.print(rsmd.getColumnName(i));
                System.out.print("(" + rsmd.getColumnTypeName(i) + ")");
                System.out.print(" | ");
            }
            System.out.println();

            while (res.next()){
                for (int i=1; i<=columnCount; i++){
                    System.out.print(res.getString(i) + " | ");
                }
                System.out.println();
            }

            long cost = System.currentTimeMillis() - start;

            result = Thread.currentThread().getName() + ": " + cost/1000.0f + "s";
            System.out.println(result);
            stmt.close();
            conn.close();

        }
}
