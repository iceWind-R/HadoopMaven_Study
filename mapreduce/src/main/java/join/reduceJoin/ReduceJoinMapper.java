package join.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/*
* product.txt   K1          V1
*               0          p0001,小米5,1000,2000
*
* orders.txt    0          1001,20180725,p0001,2
*
* ----------------------------------------------
*               K2          V2
*               p0001       p0001,小米5,1000,2000
*               p0001       1001,20180725,p0001,2
* */
public class ReduceJoinMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 判读数据来自那个文件 product 或 orders
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();// 获取文件的路径，得到文件名字
        if (name.equals("product.txt")){
            // 数据来自商品表
            // 将 K1 和 V1 转为 K2 和 V2
            String[] split = value.toString().split(",");
            String productID = split[0];

            context.write(new Text(productID),value);
        }else {
            String[] split = value.toString().split(",");
            String productID = split[2];
            context.write(new Text(productID),value);
        }
    }
}
