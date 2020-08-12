import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    // 该方法用于指定一个Job任务
    @Override
    public int run(String[] strings) throws Exception {
        // 创建一个job任务对象
        // 第一个参数是一个configuration，下面的main方法调用时已经传入，存在Configured类中，通过getConf()方法获取
        Job job = Job.getInstance(super.getConf(), "wordCount");
        // 配置job任务对象 (8个步骤)

        //1、指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://bigdata1:8020/wordcount"));
        // 本地执行方法，执行JobMain主函数即可，提前准备好文件，该路径下的wordcount.txt
        //TextInputFormat.addInputPath(job, new Path("file:///D:\\mapReduce\\input"));

        //2、指定Map阶段的处理方式 和 数据类型
        job.setMapperClass(WordCountMapper.class);
        //设置Map阶段K2的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Map阶段 V2 的类型
        job.setMapOutputValueClass(LongWritable.class);

        // 第3，4，5，6采用默认的方式

        // 7、指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //8、设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输出路径
        TextOutputFormat.setOutputPath(job,new Path("hdfs://bigdata1:8020/wordCount_out"));
        //TextOutputFormat.setOutputPath(job,new Path("file:///D:\\mapReduce\\output"));

        // 等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 启动 job 任务，实际就是调用上面的run方法
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
