package product;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        System.out.println("split[0]: "+split[0]);
        Text text = new Text();
        IntWritable intWritable = new IntWritable();

            text.set(split[0]);
            intWritable.set(1);
            context.write(text,intWritable);
    }
}
