package e4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DanJoin {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(DanJoin.class);

        job.setMapperClass(E4Mapper.class);
        job.setReducerClass(E4Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\mapreduceFile\\4\\4.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\mapreduceFile\\4\\output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
