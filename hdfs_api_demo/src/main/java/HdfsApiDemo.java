import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class HdfsApiDemo  {

    /*
    * 获取FileSystem的第一种方法
    * */
    @Test
    public void getFileSystem1() throws IOException {
        // 创建一个Configuration对象
        Configuration configuration = new Configuration();
        // 设置文件系统类型
        configuration.set("fs.defaultFS", "hdfs://bigdata1:8020");
        // 获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 输出 DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_373282607_1, ugi=11655 (auth:SIMPLE)]]
        System.out.println(fileSystem);
    }

    /*
     * 获取FileSystem的第二种方法
     * */

    @Test
    public void getFileSystem2() throws IOException, URISyntaxException, InterruptedException {
        /* 第一个参数 HDFS 的主机名 + 端口
         * 第二个参数：一个Configuration 对象
         * 第三个参数：指定用户，root用户为超级管理员
         */
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"), new Configuration(), "root");
        System.out.println(fileSystem);
        // 输出 DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_373282607_1, ugi=11655 (auth:SIMPLE)]]

        // 关闭
        fileSystem.close();
    }

    /*
     * 获取FileSystem的第三种方法
     * */
    @Test
    public void getFileSystem3() throws IOException {
        // 创建一个Configuration对象
        Configuration configuration = new Configuration();
        // 设置文件系统类型
        configuration.set("fs.defaultFS", "hdfs://bigdata1:8020");
        // 获取指定的文件系统
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        // 输出 DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_373282607_1, ugi=11655 (auth:SIMPLE)]]
        System.out.println(fileSystem);
        // 关闭
        fileSystem.close();
    }

    /*
     * 获取FileSystem的第四种方法
     * */
    @Test
    public void getFileSystem4() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://bigdata1:8020"), new Configuration());
        System.out.println(fileSystem);
    }

    /*
    * hdfs文件的遍历
    * */
    @Test
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        // 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"), new Configuration(), "root");
        // 调用方法listFiles 获取一个目录下的文件信息，为一个迭代器对象
        // 第一个参数：指定目录
        // 第二个参数，是否迭代获取
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/user/hadoop"), true);
        //遍历迭代器，获取文件的详细信息
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            // 获取文件的绝对路径："hdfs://bigdata1:8020/xxx"
            System.out.println(fileStatus.getPath() + "  ---  " + fileStatus.getPath().getName());

            //文件的Block信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("Block数：" + blockLocations.length);
        }
    }

    /*
    * hdfs创建文件夹
    * */
    @Test
    public void mkdirs() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020/a.txt"), new Configuration());
        // 创建文件夹
        boolean bl = fileSystem.mkdirs(new Path("/aaa2/bbb/ccc"));
        //创建文件
        fileSystem.create(new Path("/aaa/aaa.txt"));
        // 两个创建方法都为递归创建
        System.out.println(bl);
        fileSystem.close();
    }

    /*
    * 实现文件的下载
    * */
    @Test
    public void downloadFile() throws URISyntaxException, IOException {
        // 获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020/a.txt"), new Configuration());

        // 获取hdfs的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/a.txt"));
        // 获取本地路径的输出流
        FileOutputStream outputStream = new FileOutputStream("D://a.txt");
        // 文件的拷贝
        IOUtils.copy(inputStream, outputStream);
        // 关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }
    /*
     * 实现文件的下载 2
     * */
    @Test
    public void downloadFile2() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020/a.txt"), new Configuration());
        // 第一个参数，y要下载的HDFS文件路径
        // 第二个参数，下载到本机的目录（不是虚拟机的主机）
        // Path(String str)，路径类
        fileSystem.copyToLocalFile(new Path("/a.txt"), new Path("D://a2.txt"));
        fileSystem.close();
    }

    /*
    * 文件的上传
    * */
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020/a.txt"), new Configuration());
        /*
        * 第一个参数：本地文件路径
        * 第二个参数：要上传的HDFS目录
        * */
        fileSystem.copyFromLocalFile(new Path("D://b.txt"),new Path("/"));
        fileSystem.close(); 
    }

    /*
    * 小文件的合并
    * */
    @Test
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020/a.txt"), new Configuration(), "root");
        // 获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/big.txt"));
        // 获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        // 获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("D:\\input"));
        // 遍历每个文件，获得每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            // 将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        // 关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }

    /*
    * 文件下载到本地
    * */
    @Test
    public void urlHDFS() throws IOException {
        // 注册URL
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //获取hdfs文件的输入流
        InputStream inputStream = new URL("hdfs://bigdata1:8020/user/hadoop/dongao123.txt").openStream();
        // 获取本地文件的输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\hello.txt"));
        // 实现文件的拷贝
        IOUtils.copy(inputStream, fileOutputStream);
        // 关流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileOutputStream);
    }
}
