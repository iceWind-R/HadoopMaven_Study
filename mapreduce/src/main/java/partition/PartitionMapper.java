package partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* K1：行偏移量 LongWritable
* V1:行文本数据 Text
*
* K2:行文本数据 Text
* V2:NullWritable
* */
public class PartitionMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    // map 方法：将 K1 V1 转为 K2 V2
    // K2 就是 V1
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 方法1：定义计数器
        // 参数： 计数器类型，  计数器变量
        Counter counter = context.getCounter("MY_COUNTER", "partition_counter");
        //每次执行map方法，计数器被执行，1L 表示每次执行 +1
        counter.increment(1L);


        context.write(value,NullWritable.get());
    }
}
