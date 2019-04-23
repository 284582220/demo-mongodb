package com.ygj;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;


public class Test {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            Connection con = DriverManager.getConnection("jdbc:hive2://172.18.40.15:2181,172.18.40.16:2181,172.18.40.17:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
            Statement stmt = con.createStatement();
            String tableName = "jdbcTest";
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName +
                    " (key int, value string)");
            System.out.println("Create table success!");
            // show tables
            String sql = "show tables '" + tableName + "'";
            System.out.println("Running: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
                System.out.println(res.getString(1));
            }
            // describe table
            sql = "describe " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            sql = "select * from " + tableName;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t"
                        + res.getString(2));
            }
            sql = "select count(1) from " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }

            con.close();
        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }
    }
}
