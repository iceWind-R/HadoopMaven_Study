package partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, NullWritable> {
    /*
    * 定义分区规则
    * 返回对应分区编号
    * */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        // 拆分行文本数据(K2),获取中奖字段值
        String[] split = text.toString().split("\t");
        String numStr = split[5];
        // 判断中奖字段值和15的关系，然后返回对应的分区编号
        if (Integer.parseInt(numStr) > 15) return 1;
        else return 0;
    }
}
