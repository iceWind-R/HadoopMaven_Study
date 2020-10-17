package HDFSutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HDFSUtils {

    // 为方便管理，可将以下字段拿出作为单独的成员变量，便于后期修改
    public static final String DEFAULT_FS_NAME = "fs.defaultFS";
    public static final String DEFAULT_FS_VALUE = "hdfs://bigdata1:8020";
    public static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
    public static final String HADOOP_USER_VALUE = "root";

    // 获取FileSystem对象
    public static FileSystem getFS(){
        Configuration configuration = new Configuration();
        configuration.set(DEFAULT_FS_NAME, DEFAULT_FS_VALUE);
        System.setProperty(HADOOP_USER_NAME, HADOOP_USER_VALUE);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("获取系统文件对象失败。");
        }
        return fs;
    }

    // 关闭资源
    public static void closeFS(FileSystem fs){
        if (fs != null){
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("关闭系统文件对象失败。");
            }
        }
    }


}
