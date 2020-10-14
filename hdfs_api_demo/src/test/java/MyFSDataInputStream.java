import org.apache.hadoop.fs.*;

        import java.io.BufferedReader;
        import java.io.IOException;
        import java.io.InputStream;
        import java.io.InputStreamReader;

public class MyFSDataInputStream extends FSDataInputStream {

    private static MyFSDataInputStream myFSDataInputStream;
    private static InputStream inputStream;

    private MyFSDataInputStream(InputStream in) {
        super(in);
        inputStream = in;
    }

    public static MyFSDataInputStream getInstance(InputStream inputStream){
        if (null == myFSDataInputStream){
            synchronized (MyFSDataInputStream.class){
                if (null == myFSDataInputStream){
                    myFSDataInputStream = new MyFSDataInputStream(inputStream);
                }
            }
        }
        return myFSDataInputStream;
    }

    public static String readline(FileSystem fileStatus){
        try {
//            FSDataInputStream inputStream = fileStatus.open(remotePath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            if ((line = bufferedReader.readLine()) != null){
                bufferedReader.close();
                inputStream.close();
                return line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
