package partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionerReducer extends Reducer<Text, NullWritable, Text,NullWritable> {
    public static enum Counter{
        MY_INPUT_RECORDED,MY_INPUT_BYTES
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 方式2：使用枚举定义计数器
        context.getCounter(Counter.MY_INPUT_RECORDED).increment(1L); // 每次运行，值 +1
        context.getCounter(Counter.MY_INPUT_BYTES).increment(1L); // 每次运行，值 +1
        context.write(key, NullWritable.get());
    }
}
