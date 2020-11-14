package e1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class E1Mapper extends Mapper<Object , Text, Text , NullWritable> {
    //map将输入中的value复制到输出数据的key上，并直接输出
    private static Text newKey = new Text();      //从输入中得到的每行的数据的类型

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //获取并输出每一次的处理过程
        String line=value.toString();
        System.out.println(line);
        String arr[]=line.split("\t");
        newKey.set(arr[2]);
        context.write(newKey, NullWritable.get());
        System.out.println(newKey);

    }
}
