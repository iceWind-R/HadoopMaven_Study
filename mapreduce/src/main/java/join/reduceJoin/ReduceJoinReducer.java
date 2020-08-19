package join.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceJoinReducer extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 遍历集合,获取V3
        // 商品信息在前 first，订单信息在后 second
        String first = "";
        String second = "";
        for (Text value : values) {
            if (value.toString().startsWith("p"))
                first = value.toString();
            else
                second += value.toString() + "\t";
        }
        context.write(key, new Text(first + "\t" + second));
    }
}
