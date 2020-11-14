package e6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ReduceJoin {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ReduceJoin.class);

        job.setMapperClass(E6Mapper.class);
        job.setReducerClass(E6Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path("D:\\mapreduceFile\\6\\order_items1.txt"));
        FileInputFormat.addInputPath(job,new Path("D:\\mapreduceFile\\6\\orders1.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\mapreduceFile\\6\\output"));

        URI uri = new URI("file:///D:/mapreduceFile/6/orders1.txt");
        job.addCacheFile(uri);


        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
