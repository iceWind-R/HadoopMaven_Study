import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;
import utils.HDFSUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.Scanner;

public class Test3 {
    /*
     * 第一题
     * */
    @Test
    public void test1() throws URISyntaxException, IOException, InterruptedException {


        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"), new Configuration(), "root");
        Path srcPath = new Path("D:\\1.txt");
        Path desPath = new Path("/user/hadoop/");
        Scanner in = new Scanner(System.in);

        try {
            if (fileSystem.exists(new Path("/user/hadoop/dongao123.txt"))) {
                System.out.println("Do you want to overwrite the existed file? ( y / n )");
                String str = in.next();
                if (str.equals("y")) {
                    fileSystem.copyFromLocalFile(false, true, srcPath, desPath);
                } else {
                    FileInputStream inputStream = new FileInputStream(srcPath.toString());
                    FSDataOutputStream outputStream = fileSystem.append(new Path("/user/hadoop/dongao123.txt"));
                    byte[] bytes = new byte[1024];
                    int read = -1;
                    while ((read = inputStream.read(bytes)) > 0) {
                        outputStream.write(bytes, 0, read);
                    }
                    inputStream.close();
                    outputStream.close();
                }
            } else {
                fileSystem.copyFromLocalFile(srcPath, desPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /*
     *   第二题
     * */
    @Test
    public void test2() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"), new Configuration(), "root");
        Path remotePath = new Path("/user/hadoop/dongao123.txt");
        Path localPath = new Path("D:\\");
        try {
            if (fileSystem.exists(remotePath)) {
                fileSystem.copyToLocalFile(remotePath, localPath);
            } else {
                System.out.println("Can't find this file in HDFS!");
            }
        } catch (FileAlreadyExistsException e) {
            try {
                System.out.println(localPath.toString());
                fileSystem.copyToLocalFile(remotePath, new Path("src/test" + new Random().nextInt() + ".txt"));
            } catch (IOException e1) {
                e1.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test4() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"),new Configuration(), "root");
        Path remotePath = new Path("/user/hadoop/dongao123.txt");
        try {
            FileStatus[] fileStatus = fileSystem.listStatus(remotePath);
            for (FileStatus status : fileStatus) {
                System.out.println(status.getPermission());
                System.out.println(status.getBlockSize());
                System.out.println(status.getAccessTime());
                System.out.println(status.getPath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

@Test
    public void test5() throws URISyntaxException, IOException, InterruptedException {
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"),new Configuration(), "root");
    Path remotePath = new Path("/user/hadoop");
        try {
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(remotePath,true);
            while (iterator.hasNext()){
                FileStatus status = iterator.next();
                System.out.println(status.getPath());
                System.out.println(status.getPermission());
                System.out.println(status.getLen());
                System.out.println(status.getModificationTime());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
@Test
    public void test6() throws URISyntaxException, IOException, InterruptedException {
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"),new Configuration(), "root");
    Path remoteDirPath = new Path("/user/hadoop");
    Path remoteFilePath = new Path("/user/hadoop");
        try {
            if (fileSystem.exists(remoteDirPath)){
                System.out.println("Please choose your option: 1.create. 2.delete");
                int i = new Scanner(System.in).nextInt();
                switch (i){
                    case 1:
                        fileSystem.create(remoteFilePath);
                        break;
                    case 2:
                        fileSystem.delete(remoteDirPath,true);
                        break;
                }
            }else {
                fileSystem.mkdirs(remoteDirPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

@Test
    public void test7() throws URISyntaxException, IOException, InterruptedException {
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata1:8020"),new Configuration(), "root");
    Path remotePath = new Path("/user/hadoop");
        try {
            if (!fileSystem.exists(remotePath)){
                System.out.println("Can't find this path, the path will be created automatically");
                fileSystem.mkdirs(remotePath);
                return;
            }
            System.out.println("Do you want to delete this dir? ( y / n )");
            if (new Scanner(System.in).next().equals("y")){
                FileStatus[] iterator = fileSystem.listStatus(remotePath);
                if (iterator.length != 0){
                    System.out.println("There are some files in this dictionary, do you sure to delete all? (y / n)");
                    if (new Scanner(System.in).next().equals("y")){
                        if (fileSystem.delete(remotePath,true)){
                            System.out.println("Delete successful");
                            return;
                        }
                    }
                }
                if (fileSystem.delete(remotePath,true)){
                    System.out.println("Delete successful");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}