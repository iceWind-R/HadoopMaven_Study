package com.thorine.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * Java - API操作Hbase数据库
 *
 * @author yuanxw
 */
public class HBaseDemo {
    // Hbase数据库配置
    private Configuration conf;
    // 数据库连接
    private Connection conn;

    @Before
    public void init() {
        System.out.println("*********HbaseDemo.init()，执行开始*********");
        // 指定zk的地址，多个用“,”分割
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata1:2181,bigdata2:2181,bigdata3:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("*********HbaseDemo.init()，执行结束*********");
    }

    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void testCreateTable() throws IOException {
        System.out.println("*********HbaseDemo.testCreateTable()，执行开始*********");
        // 获得管理员
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        // 创建表
        HTableDescriptor studentTable = new HTableDescriptor(TableName.valueOf("student"));
        // 添加[student_info、school_info]列族
        HColumnDescriptor studentInfo = new HColumnDescriptor("student_info");
        HColumnDescriptor schoolInfo = new HColumnDescriptor("school_info");
        // 设置student_info 最大版本为：3
        studentInfo.setMaxVersions(3);
        // 将列族添加到表中
        studentTable.addFamily(studentInfo);
        studentTable.addFamily(schoolInfo);
        // 创建表student表
        admin.createTable(studentTable);
        // 关闭链接
        admin.close();
        System.out.println("*********HbaseDemo.testCreateTable()，执行完毕*********");
    }

    /**
     * 插入数据
     */
    @Test
    public void testPut() {
        System.out.println("*********HbaseDemo.testPut()，执行开始*********");
        try {
            // 通过表名获得表对象
            HTable table = (HTable) conn.getTable(TableName.valueOf("student"));
            // 插入rowkey行键
            Put put = new Put(Bytes.toBytes("rowkey001"));
            put.addColumn(Bytes.toBytes("student_info"), Bytes.toBytes("seq_no"), Bytes.toBytes("001"));
            put.addColumn(Bytes.toBytes("student_info"), Bytes.toBytes("real_name"), Bytes.toBytes("wangjiajia"));
            put.addColumn(Bytes.toBytes("student_info"), Bytes.toBytes("student_name"), Bytes.toBytes("王佳佳"));
            put.addColumn(Bytes.toBytes("student_info"), Bytes.toBytes("school_seq_no"), Bytes.toBytes("s001"));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("*********HbaseDemo.testPut()，执行完毕*********");
    }

    /**
     * 批量插入数据
     */
    @Test
    public void testPuts() {
        System.out.println("*********HbaseDemo.testPuts()，执行开始*********");
        // 通过表名获得表对象
        try {
            HTable table = (HTable) conn.getTable(TableName.valueOf("student"));
            List<Put> puts = new ArrayList<Put>(10000);
            for (int i = 1; i <= 1000050; i++) {
                // 插入rowkey行键
                Put put = new Put(Bytes.toBytes("rowkey" + i));
                put.addColumn(Bytes.toBytes("school_info"), Bytes.toBytes("school_seq_no"),Bytes.toBytes("seq_no" + i));
                puts.add(put);
                if (i % 10000 == 0) {
                    table.put(puts);
                    puts = new ArrayList<Put>(10000);
                }
            }
            table.put(puts);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("*********HbaseDemo.testPuts()，执行完毕*********");
    }

    /**
     * 查询当条数据
     */
    @Test
    public void testGet() {
        System.out.println("*********HbaseDemo.testGet()，执行开始*********");
        // 通过表名获得表对象
        try {
            // 获得表
            Table table = conn.getTable(TableName.valueOf("student"));
            // 通过Rowkey获得行
            Get get = new Get(Bytes.toBytes("rowkey1000"));
            Result result = table.get(get);
            // 查询Hbase获得结果
            byte[] bytes = result.getValue(Bytes.toBytes("school_info"), Bytes.toBytes("school_seq_no"));
            String str = Bytes.toString(bytes);

            // 打印输出的结果
            System.out.println(str);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("*********HbaseDemo.testGet()，执行完毕*********");
    }

    /**
     * 查询区间数据
     */
    @Test
    public void testScan() {
        System.out.println("*********HbaseDemo.testScan()，执行开始*********");
        // 通过表名获得表对象
        try {
            // 获得表
            HTable table = (HTable) conn.getTable(TableName.valueOf("student"));
            // 查询rowkey10000到rowkey15000的范围的数据
            Scan scan = new Scan(Bytes.toBytes("rowkey10000"), Bytes.toBytes("rowkey15000"));
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                byte[] bytes = result.getValue(Bytes.toBytes("school_info"), Bytes.toBytes("school_seq_no"));
                String str = Bytes.toString(bytes);
                System.out.println(str);
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("*********HbaseDemo.testScan()，执行完毕*********");
    }

    /**
     * 删除数据
     */
    @Test
    public void testDelete() {
        System.out.println("*********HbaseDemo.testDelete()，执行开始*********");
        // 通过表名获得表对象
        try {
            // 获得表
            HTable table = (HTable) conn.getTable(TableName.valueOf("student"));
            Delete delete = new Delete(Bytes.toBytes("rowkey1000050"));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("*********HbaseDemo.testDelete()，执行完毕*********");
    }
}