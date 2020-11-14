package TopN;

import java.io.File;

public class Test {
    /**
     * 迭代删除文件夹
     * @param dirPath 文件夹路径
     */
    public static void deleteDir(String dirPath)
    {
        File file = new File(dirPath);
        if(file.isFile())
        {
            file.delete();
        }else
        {
            File[] files = file.listFiles();
            if(files == null)
            {
                file.delete();
            }else
            {
                for (int i = 0; i < files.length; i++)
                {
                    deleteDir(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }

    public static void main(String[] args) {
        deleteDir("D:\\output5");
    }
}
