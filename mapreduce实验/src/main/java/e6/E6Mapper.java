package e6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class E6Mapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
        if (filePath.contains("orders1")) {
            //获取行文本内容
            String line = value.toString();
            //对行文本内容进行切分
            String[] arr = line.split("\t");
            ///把结果写出去
            context.write(new Text(arr[0]), new Text( "1+" + arr[2]+"\t"+arr[3]));
            System.out.println(arr[0] + "_1+" + arr[2]+"\t"+arr[3]);
        }else if(filePath.contains("order_items1")) {
            String line = value.toString();
            String[] arr = line.split("\t");
            context.write(new Text(arr[1]), new Text("2+" + arr[2]));
            System.out.println(arr[1] + "_2+" + arr[2]);
        }
    }
}
