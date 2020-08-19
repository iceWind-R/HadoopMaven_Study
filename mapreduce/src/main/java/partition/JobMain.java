package partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        // 创建job任务对象
        Job job = Job.getInstance(super.getConf(), "PartitionMapReduce");
        // 对job任务进行配置(8个步骤)
        // 1、设置输入类和输入的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://bigdata1:8020/input"));
        TextInputFormat.addInputPath(job,new Path("file:///D:\\input"));
        //2、设置mapper类和数据类型
        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //3、指定分区类
        job.setPartitionerClass(MyPartitioner.class);
        //4、5、6
        //7、指定reducer类和数据类型（K3 和 V3）
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置reduceTask的个数
        job.setNumReduceTasks(2);

        //8、指定输出类和输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://bigdata1:8020/out/partition_out"));
        TextOutputFormat.setOutputPath(job, new Path("file:///D:\\out\\partition_out2"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
