package com.thorine.tutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Tutorial4 {
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
    public static void putData() throws IOException {
        String tableName = "Student";
        String rowKey = "scofield";
        String cf = "score";
        String[] cn = {"English", "Math", "Computer"};
        String[] value = {"45","100", "89"};
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));// Bytes 为 hbase.utils 下的工具包，将目标类型转为字节数组

        // 给put对象赋值
        for (int i = 0; i < 3; i++) {
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn[i]),Bytes.toBytes(value[i]));
        }

        table.put(put);
        // 关闭表连接
        System.out.println("插入数据成功！");
        table.close();
    }

    // 获取数据 get
    public static void getData() throws IOException {
        String tableName = "Student";
        String rowKey = "scofield";
        String cf = "score";
        String cn = "English";
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

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


    public static void main(String[] args) throws IOException {
        //putData();
        getData();
    }
}
