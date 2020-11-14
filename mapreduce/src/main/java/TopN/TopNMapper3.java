package TopN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopNMapper3 extends Mapper<LongWritable,Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        Text text = new Text();
        IntWritable intWritable = new IntWritable();

        text.set(split[0]);
        intWritable.set(Integer.parseInt(split[1].trim()));

        //System.out.println(split[5] + " -- " +Integer.parseInt(split[3]) );

        context.write(intWritable,text);
    }
}
