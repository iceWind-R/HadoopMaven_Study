package flow.demo3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable,Text, Text, FlowBean> {
    /*
    * 将 K1 V1转换为 K2 V2
    * K1            V1
    * 0             该行全部数据
    * --------------------
    * K2            V2
    * 手机号        四个值
    * */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、拆分行文本数据，得到手机号 --> K2
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];
        // 2、创建FlowBean对象，并从行文本数据拆分出流量的四个字段，并将四个流量字段的值赋值给FlowBean对象
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));

        // 将 K2 和 V2写入上下文
        context.write(new Text(phoneNum), flowBean);
    }
}
