package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        // 创建job对象
        Job job = Job.getInstance(super.getConf(), "mapReduce_sort");
        // 配置job任务(8个步骤)
        // 创建job任务对象
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://bigdata1:8020/input/sort_input"));
        //TextInputFormat.addInputPath(job,new Path("file:///D:\\input"));
        //2、设置mapper类和数据类型
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);
        //3、4、5、6,虽然本程序侧重于排序，但在 SortBean中已经指定排序规则，所以此处无需添加
        //7、指定reducer类和数据类型（K3 和 V3）
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);

        //8、指定输出类和输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://bigdata1:8020/out/sort_out"));
        //TextOutputFormat.setOutputPath(job, new Path("file:///D:\\out\\partition_out2"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
