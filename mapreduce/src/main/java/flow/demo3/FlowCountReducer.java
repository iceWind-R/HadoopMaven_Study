package flow.demo3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Integer upFlow = 0;         // 上行数据包数
        Integer downFlow = 0;       // 下行数据包数
        Integer upCountFlow = 0;    // 上行流量总数
        Integer downCountFlow = 0;  // 下行流量总数

        // 1、遍历集合，并将集合中对应的四个字段累加求和
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        // 2、创建FlowBean对象，并给对象赋值
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        // 3、将 K3 和 V3
        context.write(key,flowBean);
    }
}
