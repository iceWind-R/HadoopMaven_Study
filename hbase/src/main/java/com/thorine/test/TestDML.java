package com.thorine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

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
public class TestDML {

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

    // 向表中插入数据
    // 参数：表名，rowKey，列族，列名，value
   public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        // 获取表对象
       Table table = connection.getTable(TableName.valueOf(tableName));

       // 创建put对象
       Put put = new Put(Bytes.toBytes(rowKey));// Bytes 为 hbase.utils 下的工具包，将目标类型转为字节数组

       // 给put对象赋值
       put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

       // 若想添加多个列，多调用下面方法即可
       // put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
       // put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

       // 插入数据
       // 若想添加多个RowKey，只需创建多个Put对象，放到一个集合里，再用下面的put方法，插入一个集合对象。
       table.put(put);
       // 关闭表连接
       table.close();
   }

   // 获取数据 get
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 指定获取的列族
        // get.addFamily(Bytes.toBytes(cf));
        // 指定获取的列族 和 列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        // 设置获取数据的版本数
        get.setMaxVersions(5);

        // 获取数据
        Result result = table.get(get);

        // 解析result
        for (Cell cell : result.rawCells()) {
            System.out.print("ROW:" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.print(", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.print(", VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println(", CF:" + Bytes.toString(CellUtil.cloneFamily(cell)));
        }

        // 关闭表连接
        table.close();
    }

    // 获取数据 scan
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 构建Scan对象
        // Scan scan = new Scan();
        // 指定RowKey范围从 10010 到 1003 ，左闭右开
        Scan scan = new Scan(Bytes.toBytes("10010"),Bytes.toBytes("1008"));
        // 扫描表
        ResultScanner resultScanner = table.getScanner(scan);
        //解析
        for (Result result : resultScanner) {
            // 打印
            for (Cell cell : result.rawCells()) {
                System.out.print("ROW:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print(", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.print(", VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(", CF:" + Bytes.toString(CellUtil.cloneFamily(cell)));

            }
        }
        table.close();
    }

    public static void deleteData(String tableName,String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 构造删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 设置删除的列
        // delete.addColumn();  // 删除最新的版本 ，十分诡异，慎用

        // 给指定的列删除所有的版本，若加上第三个参数时间戳，则表示删除指定时间戳以前的数据
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));

        // 删除指定的列族
        delete.addFamily(Bytes.toBytes(cf));

        // 执行删除操作
        table.delete(delete);
        table.close();
    }


    public static void main(String[] args) throws IOException {
        // putData("stu","1004","info1","name","dongao");
//        getData("student","1001","info","name");
        scanTable("stu");

       // deleteData("stu","1002","","");
        close();


    }
}
