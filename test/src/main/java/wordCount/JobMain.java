package wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "worldCount");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://bigdata1:8020/worldcount"));

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输出路径，并判断该目录是否存在，存在则删除
        Path path = new Path("hdfs://bigdata1:8020/wordCount_out");
        TextOutputFormat.setOutputPath(job,path);
        //TextOutputFormat.setOutputPath(job,new Path("file:///D:\\mapReduce\\output"));

        // 判断目标目录是否存在
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020/"), new Configuration());
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
        }

        // 等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration,new JobMain(),args);
        System.exit(run);
    }
}
