package product;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        IntWritable intWritable = new IntWritable();

        for (IntWritable value : values) {
            count += value.get();
        }

        intWritable.set(count);
        context.write(key,intWritable);
    }
}
