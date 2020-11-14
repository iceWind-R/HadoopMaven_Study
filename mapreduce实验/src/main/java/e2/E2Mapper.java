package e2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class E2Mapper extends Mapper<Object , Text , Text, IntWritable> {

    private static Text newKey=new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将输入的纯文本文件的数据转化成 String
        String line=value.toString();
        System.out.println(line);
        String arr[]=line.split("\t");
        newKey.set(arr[1]);
        int click=Integer.parseInt(arr[2]);
        context.write(newKey, new IntWritable(click));

    }
}
