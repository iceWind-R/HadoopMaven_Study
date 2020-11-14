package wordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
* 四个泛型解释：
* KEYIN:K2类型
* VALUEIN:V2类型
* KEYOUT：K3类型
* VALUEOUT：V3类型
* */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    /*
    * reduce方法作用：将新的 K2 和 V2 转换为 K3 和 V3，并写入上下文
    *
    * 参数：
    *   key：新 K2
    *   values：集合 新V2
    *
    * 如何将新的 K2 和 V2 转换为 K3 和 V3
    * 新 K2          V2
    *   world       <1,1,1>
    *   hello       <1,1>
    *   hadoop      <1>
    * ---------------------
    *   K3          V3
    *   hello       2
    *   world       3
    *   hadoop      1
    * */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        LongWritable longWritable = new LongWritable();

        // 遍历values集合，将集合中的数字相加得到 V3
        for (LongWritable value : values) {
            count += value.get();
        }
        // 将 K3 和 V3 写入上下文中
        longWritable.set(count);
        context.write(key, longWritable);
    }
}
