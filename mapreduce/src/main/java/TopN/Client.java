package TopN;

import java.io.*;
import java.util.Scanner;

public class Client {
    // m 表示文件位置，n表示topN
    public static void getTopN(int m, int n) throws IOException {
        FileInputStream fis = new FileInputStream("D:\\output" + m + "\\part-r-00000");
        // 防止路径乱码   如果utf-8 乱码  改GBK     eclipse里创建的txt  用UTF-8，在电脑上自己创建的txt  用GBK
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        BufferedReader br = new BufferedReader(isr);

        String line = "";
        String[][] str = new String[1000][2];
        int i = 0;

        while ((line = br.readLine()) != null) {
            String[] split = line.split("\t");
            str[i] = split;
        }

        br.close();
        isr.close();
        fis.close();
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        int m = 0;
        while (m!=4) {
            System.out.println("==================菜单======================");
            System.out.println("1、统计最受欢迎的视频/文章的TopN访问次数");
            System.out.println("2、按照地市统计最受欢迎的TopN课程");
            System.out.println("3、按照流量统计最受欢迎的TopN课程");
            System.out.println("4、退出");
            System.out.println("请选择命令：");
            Scanner in = new Scanner(System.in);
            m = in.nextInt();
            System.out.println("请输入n值：");
            int n = in.nextInt();
            switch (m) {
                case 1:
                    Driver.method();
                    getTopN(m,n);
                    break;
                case 2:
                    Driver2.method();
                    getTopN(m,n);
                    break;
                case 3:
                    Driver1.method();
                    getTopN(m,n);
                    break;
                case 4:
                    break;
                default:
                    System.out.println("输入命令有误，请重新输入：");
                    break;
            }
        }
        System.out.println("感谢光临，再见！");
    }
}
