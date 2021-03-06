package commonFriends.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class Step2Mapper extends Mapper<LongWritable, Text,Text,Text> {
    /*
    * K1            V1
    * 0             A-F-C-J-E   B
    * ---------------------------
    * K2            V2
    * A-C           B
    * A-E           B
    * A-F           B   A、F的共同好友是 B
    *
    * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分行文本数据，结果的第二部分 得到 V2
        String[] split = value.toString().split("\t");
        String friendStr = split[1];
        // 继续以'-'为分割符拆分文本数据第一部分，得到数组
        String[] userArray = split[0].split("-");

        // 对数据进行排序
        Arrays.sort(userArray);
        // 对数组中的元素进行两两组合，得到 K2
        /*
        * A-E-C -> A C E
        * AC AE CE
        * 两层for循环
        * */
        for (int i = 0; i < userArray.length - 1; i++) {
            for (int j = i + 1; j < userArray.length; j++) {
                context.write(new Text(userArray[i] + "-" + userArray[j]), new Text(friendStr));
            }
        }

    }
}
