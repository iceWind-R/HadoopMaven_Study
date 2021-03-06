package TopN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver1 {
    public static void method() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Driver1.class);

        job.setMapperClass(TopNMapper1.class);
        job.setReducerClass(TopNReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\result.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output3"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        method();
    }
}
