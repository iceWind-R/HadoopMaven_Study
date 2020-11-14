package e2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class E2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int num=0;
        int count=0;
        for(IntWritable val:values){
            num+=val.get(); //每个元素求和num
            count++;        //统计元素的次数count
        }
        int avg=num/count;  //计算
        context.write(key,new IntWritable(avg));

    }
}
