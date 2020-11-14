package e7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class E7Mapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
    private final IntPair intkey = new IntPair();
    private final IntWritable intvalue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int left = 0;
        int right = 0;
        if (tokenizer.hasMoreTokens())
        {
            left = Integer.parseInt(tokenizer.nextToken());
            if (tokenizer.hasMoreTokens())
                right = Integer.parseInt(tokenizer.nextToken());
            intkey.set(right, left);
            intvalue.set(left);
            context.write(intkey, intvalue);
        }

    }
}
