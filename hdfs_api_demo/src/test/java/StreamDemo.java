import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.HDFSUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class StreamDemo {
    FileSystem fs = null;

    @Before
    public void init(){
        fs = HDFSUtils.getFS();
    }

    @After
    public void close(){
        HDFSUtils.closeFS(fs);
    }

    /* 上传文件 , 通过 IOUtils工具包 */
    @Test
    public void uploadFile() throws IOException {
        // 创建 hdfs 文件输出流，通过此 流 写入文件
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/user/hadoop/dongao1.txt")); // 一个新路径，创建并写入该文件
        // 获取本地文件输入流，将本地文件读入内存
        FileInputStream inputStream = new FileInputStream("D:\\dongao.txt");
        // 通过IOUtils 的 copyBytes 执行读写
        // 参数： 输入流，输出流，每次读取字节的个数
        IOUtils.copyBytes(inputStream, fsDataOutputStream,1024);

        // 调用 IOUtils 的关闭流方法
        IOUtils.closeStream(fsDataOutputStream);
        IOUtils.closeStream(inputStream);
    }

    /* 将HDFS的文件下载到本地 */
    @Test
    public void downloadFile() throws IOException {
        // 先打开一个HDFS中存在的文件输入流
        FSDataInputStream inputStream = fs.open(new Path("/user/hadoop/dongao.txt"));
        // 获取一个文件的输出流，将内容写入本地
        FileOutputStream outputStream = new FileOutputStream("D:\\董奥.txt");
        // 执行读写，读取输入流的内容，写入到输出流，单个文件大小4096个字节
        IOUtils.copyBytes(inputStream,outputStream,4096);
        outputStream.flush(); // 手动刷新

        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
    }

    /* 将文件输出到控制台 */
    @Test
    public void cat() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/user/hadoop/dongao.txt"));
        IOUtils.copyBytes(inputStream, System.out, 4096);
        IOUtils.closeStream(inputStream);
    }
}
