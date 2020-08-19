package commonFriends.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step1Mapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 以冒号拆分行文本数据：冒号左边
        String[] split = value.toString().split(":");
        String userStr = split[0];
        // 将冒号右边的字符串以逗号拆分，每个成员就是 K2
        String[] split1 = split[1].split(",");
        for (String k2 : split1) {
            // 将K2 V2 写入上下文
            context.write(new Text(k2), new Text(userStr));
        }
    }
}
