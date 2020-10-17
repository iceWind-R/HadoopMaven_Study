import utils.HDFSUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.util.Scanner;


public class EditMachine {

    public FileSystem fs = null;

    /*
    * 根据指定参数 创建 相应文件
    * */
    public void mkdir(String path) {
        fs = HDFSUtils.getFS();
        try {
            fs.create(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        HDFSUtils.closeFS(fs);
    }

    /*
    * 根据指定参数 查看 相应文件
    * */
    public void cat(String path) {
        fs = HDFSUtils.getFS();
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(new Path(path));
            IOUtils.copyBytes(inputStream, System.out, 4096);

        } catch (IOException e) {
            e.printStackTrace();
        }
            IOUtils.closeStream(inputStream);
            HDFSUtils.closeFS(fs);
    }

    public void edit(){
        System.out.println("请输入你想追加的内容：");
        Scanner in = new Scanner(System.in);
        String content = in.next();
        System.out.print("请选择追加到开头（1）还是结尾（2）：");
        int n = in.nextInt();
        System.out.println(content + " - " + n);

    }

    public void editFile() throws IOException {
        fs = HDFSUtils.getFS();
        String destPath = "/user/hadoop/dongao.txt";
        String neirong;
        String newPath = "/user/hadoop/dongao.txt";
        int n;
        Scanner sc = new Scanner(System.in);
//        System.out.printf("请输入要编辑的目录及文件名:");
//        destPath = sc.next();
        System.out.println("请输入要追加的内容:");
        neirong = sc.next();
        System.out.println("请选择要追加的位置:");
        System.out.println("1.开头");
        System.out.println("2.结尾");
        n = sc.nextInt();
        if (n == 2){ // 追加到文件结尾，直接调用append方法
            FSDataOutputStream append = fs.append(new Path(destPath));
            append.write(neirong.getBytes());
            IOUtils.closeStream(append);
        }else {
            // 源文件的输入流
            FSDataInputStream open = fs.open(new Path(destPath));
            // 临时存放文件的输出流
            FSDataOutputStream tempOut = fs.create(new Path("/temp.txt"),true);
            // 把要添加到开头的内容 写到输出流中
            tempOut.write(neirong.getBytes());
            // 把输入流 输入到 输出流 的后面
            IOUtils.copyBytes(open,tempOut,fs.getConf());
            IOUtils.closeStream(open);
            IOUtils.closeStream(tempOut);

            FSDataOutputStream outputStream = fs.create(new Path(destPath));
            FSDataInputStream open1 = fs.open(new Path("/temp.txt"));
            IOUtils.copyBytes(open1,outputStream,fs.getConf());
            IOUtils.closeStream(open1);
            IOUtils.closeStream(outputStream);
        }
//        System.out.println("请输入要保存的位置:");
//        newPath = sc.next();
//        fs.rename(new Path(destPath),new Path(newPath));
    }

    public static void main(String[] args){
        EditMachine editM = new EditMachine();
        System.out.println("---------HDFS文本编辑器---------");
        System.out.println("1、新建");
        System.out.println("2、打开");
        System.out.println("3、编辑");
        System.out.println("4、保存");
        System.out.println("5、退出");
        System.out.print("请选择：");
        int n = 0;
        Scanner in = new Scanner(System.in);
        n = in.nextInt();
        while (n != 5) {
            switch (n) {
                case 1:
                    editM.mkdir("/user/hadoop/1015.txt");
                    break;
                case 2:
                    editM.cat("/user/hadoop/dongao.txt");
                    break;
                case 3:
                    try {
                        editM.editFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case 4:

                    break;
                default:
                    System.out.println("输入命令代号有误。");
                    break;
            }
            System.out.print("请选择：");
            n = in.nextInt();
        }
    }
}
