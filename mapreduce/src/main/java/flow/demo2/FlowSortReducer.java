package flow.demo2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSortReducer extends Reducer<FlowBean,Text, Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 遍历集合,取出K3，并将 K3 和 V3 写入上下文
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
