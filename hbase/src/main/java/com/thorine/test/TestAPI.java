package com.thorine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/*
* DDL:
*   1、判断表是否存在
*   2、创建表
*   3、创建namespace
*   4、删除表
* DML：
*   5、插入数据
*   6、查询数据（get）
*   7、查询（scan）
*   8、删除数据
* */
public class TestAPI {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "bigdata1:2181,bigdata2:2181,bigdata3:2181");
            // 创建连接对象
            connection = ConnectionFactory.createConnection(configuration);
            // 创建管理员对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 关闭资源
    public static void close(){
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 1、判断表是否存在
    public static boolean isTableExist(String tableName) {
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b;
    }

    public static void main(String[] args) {
        System.out.println(isTableExist("student"));

        close(); // 关闭资源
    }
}
