import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* 其中 Mapper类的四个泛型解释：
* KETIN：K1的类型
* VALUEIN:V1的类型
* KEYOUT:K2的类型
* VALUEOUT:V2的类型
*
* 使用已经给出定义好的类型，基本类型的封装，操作序列化起来更加方便
* <Long, String, Long, String>  - > <LongWritable, Text, Text, LongWritable>
* */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /* map方法就是将 K1 和 V1 转换为 K2 和 V2
    * 参数：
    *   key  : K1 行偏移量
    *   value: V1 每一行的文本数据
    *   context : 上下文对象
    *
    * 如何将 K1 和 V1 转换为 K2 和 V2
    * K1            V1
    * 0         hello,world,hadoop
    * 18        hdfs,hello,hadoop
    * ---------------------------
    * K2           V2
    * hello         1
    * world         1
    * hadoop        1
    * hdfs          1
    * hello         1
    * hadoop        1
    *
    */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        // 将一行的文本数据进行拆分
        String[] split = value.toString().split(",");
        // 遍历数组，组装K2 和 V2
        for (String word : split) {
            text.set(word);
            longWritable.set(1);
            // 将 K2 和 V2 写入上下文
            context.write(text, longWritable);
        }
    }
}
