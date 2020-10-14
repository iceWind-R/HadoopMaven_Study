import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.HDFSUtils;

import java.io.IOException;

public class Test01 {
    FileSystem fs = null;

    // 在所有功能执行前拿到fileSystem对象即可
    @Before // 在程序测试前执行，只执行一次，可以用于工程的初始化
    public void init(){
        fs = HDFSUtils.getFS();
    }

    // 在所有功能执行完毕后能够关闭FileSystem对象
    @After //在程序测试后执行，只执行一次，可以用于关闭资源等收尾工作
    public void close(){
        HDFSUtils.closeFS(fs);
    }

    // 要测试的类直接加上 @Test 注解即可。
    @Test
    public void mkdir() throws IOException {
        boolean b = fs.mkdirs(new Path("\\user\\hadoop\\test"));
        if (b) {
            System.out.println("创建成功");
        } else {
            System.out.println("创建失败");
        }
    }

    // 创建文件并写入数据，文件存在则覆盖
    @Test
    public void touch() throws IOException {
        // 操作文件系统通过 FSDataOutputStream 流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("\\user\\hadoop\\test\\data.dat"));
        fsDataOutputStream.write("Hello,BigData1111111111111".getBytes()); // 默认字节流
        // 关闭流
        fsDataOutputStream.close();
    }

    @Test
    public void listFiles() throws IOException {
        // 调用方法listFiles 获取一个目录下的文件信息，为一个迭代器对象
        // 第一个参数：指定目录
        // 第二个参数，是否迭代获取
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/user/hadoop"), true);
        while (listFiles.hasNext()) {
            // 每个迭代器元素为一个 LocatedFileStatus 对象，存储着文件的信息
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("Name: " + fileStatus.getPath().getName());
            System.out.println("Len: " + fileStatus.getLen());  // 文件长度
            System.out.println("BlockSize: " + fileStatus.getBlockSize());  // Block 块大小
            System.out.println("Replication: " + fileStatus.getReplication()); // 副本数

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation blk : blockLocations) {
                System.out.println("blk-length:" + blk.getLength() + " - blk-offset: " + blk.getOffset());

                String[] hosts = blk.getHosts(); // 文件块副本所在的主机
                for (String host : hosts) {
                    System.out.println("host: " + host);
                }
            }
            System.out.println("-------------------end...----------------------");
        }
    }
}
