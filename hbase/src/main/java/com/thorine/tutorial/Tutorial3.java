package com.thorine.tutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Tutorial3 {
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

    public static void createTable(String tableName,  String... cfs) throws IOException {
        if (isTableExist(tableName)) {
            // 使表下线
            admin.disableTable(TableName.valueOf(tableName));
            // 删除表
            admin.deleteTable(TableName.valueOf(tableName));

            System.out.println(tableName + " 表存在，已被删除。");
        }

        // 判断是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息");
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

    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 创建put对象
        Put put = new Put(Bytes.toBytes(row));// Bytes 为 hbase.utils 下的工具包，将目标类型转为字节数组

        int length = fields.length;
        for (int i = 0; i < length; i++) {
            // 切割 field 数组，{“Score:Math”, ”Score:Computer Science”, ”Score:English”}
            String[] cfcns = fields[i].split(":");

            put.addColumn(Bytes.toBytes(cfcns[0]),Bytes.toBytes(cfcns[1]),Bytes.toBytes(values[i]));
        }
        table.put(put);
        // 关闭表连接
        table.close();
    }


    // 格式化输出
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
            System.out.println();
        }
    }

    /**
     * 浏览表 tableName 某一列的数据，如果某一行记录中该列数据不存在，则返回 null。
     * 要求当参数 column 为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列的数据；
     * 当参数 column 为某一列具体名称（例如“Score:Math”）时，只需要列出该列的数据。
     */
    public static void scanColumn (String tableName,String column) throws IOException
    {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String [] cols = column.split(":");
        if(cols.length==1)
        {
            scan.addFamily(Bytes.toBytes(column));
        }
        else {
            scan.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes(cols[1]));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result !=null;result = scanner.next()) {
            showCell(result);
        }
        table.close();
        close();
    }



    public static void modifyData(String tableName, String row, String column) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 创建put对象
        Put put = new Put(Bytes.toBytes(row));// Bytes 为 hbase.utils 下的工具包，将目标类型转为字节数组

        String[] cfcns = column.split(":");

        put.addColumn(Bytes.toBytes(cfcns[0]),Bytes.toBytes(cfcns[1]),Bytes.toBytes("mmmmodify")); // 修改数据写死
        table.put(put);
        // 关闭表连接
        table.close();
    }

    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 构造删除对象
        Delete delete = new Delete(Bytes.toBytes(row));

        // 执行删除操作
        table.delete(delete);
        table.close();
    }


    public static void main(String[] args) throws IOException {
        String tableName = "test";
        String[] fields = {"Score:Math","Score:Computer Science","Score:English"};
        String[] values = {"88","89","90"};

        //createTable("test","Score");

        //addRecord(tableName,"1005",fields,values);
        //addRecord(tableName,"1006",fields,values);

        scanColumn(tableName,"Score:Math"); // column，可以是列族，则显示该列族下的所有列数据，可以是某个列，则显示该信息即可

        //modifyData(tableName,"1002","Score:Computer Science");

        //deleteRow(tableName,"1001");

        close();
    }
}
