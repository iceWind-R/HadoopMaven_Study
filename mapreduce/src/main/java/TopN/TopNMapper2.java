package TopN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopNMapper2 extends Mapper<LongWritable,Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // split 6 个字段，ip，time，day, traffic,type,id
        String[] split = value.toString().split(",");

        //String[] str = {split[0],split[3],split[4],split[5]}; // ip,traffic,type,id

        Text text = new Text();
        IntWritable intWritable = new IntWritable();

        text.set(split[0]);
        intWritable.set(1);

        //System.out.println(split[5] + " -- " +Integer.parseInt(split[3]) );

        context.write(text,intWritable);
    }
}
