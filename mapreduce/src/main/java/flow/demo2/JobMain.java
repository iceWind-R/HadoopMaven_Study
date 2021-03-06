package flow.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        // 创建job对象
        Job job = Job.getInstance(super.getConf(), "mapReduce_flowSort");
        // 配置job任务(8个步骤)
        // 创建job任务对象
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job,new Path("hdfs://bigdata1:8020/input/sort_input"));
        TextInputFormat.addInputPath(job,new Path("file:///D:\\out\\flowCount_out"));
        //2、设置mapper类和数据类型
        job.setMapperClass(FlowSortMapper.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(Text.class);

        //7、指定reducer类和数据类型（K3 和 V3）
        job.setReducerClass(FlowSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //8、指定输出类和输出类型
        job.setOutputFormatClass(TextOutputFormat.class);

        // 需要的源数据是从上一个程序得来的
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://bigdata1:8020/out/sort_out"));
        TextOutputFormat.setOutputPath(job, new Path("file:///D:\\out\\flowSort_out"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
