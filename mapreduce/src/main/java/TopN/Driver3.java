package TopN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class Driver3 {
    public static void deleteDir(String dirPath)
    {
        File file = new File(dirPath);
        if(file.isFile())
        {
            file.delete();
        }else
        {
            File[] files = file.listFiles();
            if(files == null)
            {
                file.delete();
            }else
            {
                for (int i = 0; i < files.length; i++)
                {
                    deleteDir(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }

    public static void method(int s) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Driver3.class);

        job.setMapperClass(TopNMapper3.class);
        job.setReducerClass(TopNReducer3.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        String path = "D:\\output" + s + "\\part-r-00000";
        FileInputFormat.setInputPaths(job,new Path(path));
        //deleteDir("D:\\output" + s);
        FileOutputFormat.setOutputPath(job,new Path("D:\\output0" + s));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
            method(3);

    }
}
