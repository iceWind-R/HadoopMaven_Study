package e3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class E3Reducer extends Reducer<IntWritable,Text, IntWritable, Text> {
    // 声明对象 result
    private static IntWritable result = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key,value);
        }
    }
}
