package operation;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

public class ShowContent {

    public static void main(String[] args) throws IOException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        InputStream inputStream = new URL("hdfs", "bigdata1", 8020,"/user/hadoop/dongao123.txt").openStream();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = bufferedReader.readLine()) != null){
            System.out.println(line);
        }
    }
}
