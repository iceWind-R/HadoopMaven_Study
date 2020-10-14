package operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
 * 实现本地的文件上传到HDFS上，若HDFS不存在，则创建新文件，若存在，则选择　覆盖　或是　追加
 * */
public class CopyFile {
     //判断路径是否存在
    public static boolean PathIsExit(Configuration configuration, String path){
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            return fileSystem.exists(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /*
     * 指定路径不存在，复制文件到指定路径
     * */
    public static void copyFromLocalFile(Configuration configuration ,String localFilePath, String remoteFilePath) {
        System.out.println("copyFromLocalFile");
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"),configuration,"root")) {
            /* fs.copyFromLocalFile 第一个参数表示是否删除源文件，第二个参数表示是否覆盖 */
            fileSystem.copyFromLocalFile(false, true, localPath, remotePath);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*
     * 追加文件内容
     * */
    public static void appendToFile(Configuration configuration, String localFilePath, String remoteFilePath) {
        System.out.println("appendToFile");
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"),configuration,"root");
            FileInputStream in = new FileInputStream(localFilePath)) {
            FSDataOutputStream out = fileSystem.append(remotePath);
            byte[] data = new byte[1024];
            int read;
            while((read = in.read(data)) > 0) {
                out.write(data,0, read);
            }
            out.close();
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://bigdata1:8020");

        String localFilePath = "D:\\dongao.txt";    // 本地路径
        String remoteFilePath = "/user/hadoop/dongao.txt";        // HDFS路径
        //String choice = "append";
        String choice = "overwrite";


        try {
            /* 判断文件是否存在 */
            boolean fileExists = false;
            if (CopyFile.PathIsExit(configuration, remoteFilePath)) {
                fileExists = true;
                System.out.println(remoteFilePath + " 已存在.");
            } else {
                System.out.println(remoteFilePath + " 不存在.");
            }
            /* 进行处理 */
            if (!fileExists) { // 文件不存在，则上传
                CopyFile.copyFromLocalFile(configuration, localFilePath, remoteFilePath);
                System.out.println(localFilePath + " 已上传至 " + remoteFilePath);
            } else if (choice.equals("overwrite")) { // 选择覆盖
                CopyFile.copyFromLocalFile(configuration, localFilePath, remoteFilePath);
                System.out.println(localFilePath + " 已覆盖 " + remoteFilePath);
            } else if (choice.equals("append")) { // 选择追加
                CopyFile.appendToFile(configuration, localFilePath, remoteFilePath);
                System.out.println(localFilePath + " 已追加至 " + remoteFilePath);
            } else {
                System.out.println("What ... ? ? ? ");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
















