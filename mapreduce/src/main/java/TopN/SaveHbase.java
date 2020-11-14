package TopN;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SaveHbase {
    public static void main(String[] args) throws IOException {
        // 文件夹路径
        String path = "D:\\result.txt";

        // 建表
        TestDDL.createTable("topn","info");
readFile02(path);
    }



    /**
     * 读取一个文本 一行一行读取
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static void readFile02(String path) throws IOException {
        // 使用一个字符串集合来存储文本中的路径 ，也可用String []数组
        List<String> list = new ArrayList<String>();
        FileInputStream fis = new FileInputStream(path);
        // 防止路径乱码   如果utf-8 乱码  改GBK     eclipse里创建的txt  用UTF-8，在电脑上自己创建的txt  用GBK
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        BufferedReader br = new BufferedReader(isr);
        String line = "";
        int rowKey = 0;
        while ((line = br.readLine()) != null) {
            // 如果 t x t文件里的路径 不包含---字符串       这里是对里面的内容进行一个筛选

            String[] split = line.split(",");
//            TestDML.putData("topn",String.valueOf(rowKey),"info","ip",split[0]);
//            TestDML.putData("topn",String.valueOf(rowKey),"info","time",split[1]);
//            TestDML.putData("topn",String.valueOf(rowKey),"info","day",split[2]);
//            TestDML.putData("topn",String.valueOf(rowKey),"info","traffic",split[3]);
//
//            TestDML.putData("topn",String.valueOf(rowKey),"info","type",split[4]);
//            TestDML.putData("topn",String.valueOf(rowKey),"info","id",split[5]);

            if (rowKey < 100) {
                System.out.println(split[3].trim());
            }
            rowKey ++;
        }
        br.close();
        isr.close();
        fis.close();
    }
}
