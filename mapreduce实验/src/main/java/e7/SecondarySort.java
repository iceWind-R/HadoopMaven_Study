package e7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class SecondarySort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(E7Mapper.class);
        job.setReducerClass(E7Reducer.class);

        job.setPartitionerClass(FirstPartitioner.class);

        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\mapreduceFile\\7\\7.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\mapreduceFile\\7\\output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

}
