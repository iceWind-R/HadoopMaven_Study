package e3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OneSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(OneSort.class);

        job.setMapperClass(E3Mapper.class);
        job.setReducerClass(E3Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\mapreduceFile\\3\\3.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\mapreduceFile\\3\\output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
