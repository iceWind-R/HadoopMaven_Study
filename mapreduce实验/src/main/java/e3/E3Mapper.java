package e3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class E3Mapper extends Mapper<Object, Text, IntWritable, Text> {

    private static Text goods = new Text(); // 商品ID
    private static IntWritable num = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String arr[]=line.split("\t");
        num.set(Integer.parseInt(arr[2]));
        goods.set(arr[1]);
        context.write(num,goods);
    }
}
