package e4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class E4Mapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");   //按行截取
        String mapkey=arr[1];
        String mapvalue=arr[2];
        String relationtype=new String();  //左右表标识
        relationtype="1";  //输出左表
        context.write(new Text(mapkey),new Text(relationtype+"+"+mapvalue));
        //System.out.println(relationtype+"+"+mapvalue);
        relationtype="2";  //输出右表
        context.write(new Text(mapvalue),new Text(relationtype+"+"+mapkey));
        //System.out.println(relationtype+"+"+mapvalue);

    }
}
