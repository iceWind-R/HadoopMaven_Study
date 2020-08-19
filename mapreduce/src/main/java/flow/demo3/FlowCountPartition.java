package flow.demo3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartition extends Partitioner<Text,FlowBean> {
    /*
    * 该方法用来指定一个分区文件
    * 135 开头的电话
    * 136 ...
    * 137 ...
    *
    * 参数:
    * text: K2
    * flowBean: V2
    * int i
    * */

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // 获取手机号
        String phoneNum = text.toString();
        // 判断手机号什么开头，返回对应的分区编号
        if (phoneNum.startsWith("135"))
            return 0;
        else if (phoneNum.startsWith("136"))
            return 1;
        else if (phoneNum.startsWith("137"))
            return 2;
        else return 3;
    }
}
