package com.thorine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
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
public class TestDDL {
    /*
    * Connection代表客户端和集群的一个连接,这个连接包含对master的连接，和zk的连接
    * Connection的创建是重量级的，因此建议一个应用只创建一个Connection对象.
    * Connection是线程安全的，可以在多个线程中共享同一个Connection实例.
    *
    * 从Connection中获取Table和Admin对象的实例！Table和Admin对象的创建是轻量级，且不是线程安全的！
    * 因此不建议池化或缓存Table和Admin对象的实例，每个线程有自己的Table和Admin对象的实例！
    * */

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
    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }


    //2、创建表
    // 第一个参数表明，第二个为可变参数，列族信息
    public static void createTable(String tableName, String... cfs) throws IOException {
        // 判断是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }
        // 判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + " 表 已存在。");
            return;
        }

        // 3、创建表
        // 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 循环添加列族信息
        for (String cf : cfs) {
            // 创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
         // hColumnDescriptor.setMaxVersions(5); // 版本数

            // 添加具体列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        // 创建表
        admin.createTable(hTableDescriptor);
    }

    //4、删除表
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)) {
            System.out.println(tableName + " 表不存在，无法删除！");
            return;
        }
        // 使表下线
        admin.disableTable(TableName.valueOf(tableName));
        // 删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    // 创建命名空间
    public static void createNamespace(String nsName) {
        // 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nsName).build();

        // 创建命令空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(nsName + " 命令空间已存在！");
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("命令空间已存在，但我仍可以走到这！！");

    }


    public static void main(String[] args) throws IOException {
        System.out.println(isTableExist("stu1"));
        // 这个表名表示在命名空间0408下创建表stu1
        createTable("0408:stu1","info1","info2");

        dropTable("stu1");

        createNamespace("0408");

        System.out.println(isTableExist("stu1"));

        close(); // 关闭资源
    }
}
