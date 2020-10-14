import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Test2 {
    public static void cat(Configuration conf, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fs = FileSystem.get(conf);
             FSDataInputStream in = fs.open(remotePath);
             BufferedReader d = new BufferedReader(new InputStreamReader(in)) ) {
            String line;
            while ((line = d.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata1:8020");
        String remoteFilePath = "/user/hadoop/dongao123.txt"; // HDFS路径
        try {
            System.out.println("读取文件: " + remoteFilePath);
            Test2.cat(conf, remoteFilePath);
            System.out.println("\n读取完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
