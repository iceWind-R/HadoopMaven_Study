package wordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        LongWritable longWritable = new LongWritable();
        for (LongWritable value : values) {
            count += value.get();
        }
        longWritable.set(count);
        context.write(key, longWritable);
    }
}
