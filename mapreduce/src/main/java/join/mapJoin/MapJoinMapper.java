package join.mapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable,Text,Text, Text> {
    private HashMap<String, String> map = new HashMap<>();

    // 第一件事情：将分布式缓存的小表数据读取到本地Map集合(只需要做一次)
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取分布式缓存文件列表
        URI[] cacheFiles = context.getCacheFiles();
        // 获取指定的缓存文件的文件系统(FileSystem)
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
        // 获取文件的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
        // 读取文件内容,并将数据存入本地map集合
        // 将字节输入流转为字符缓冲流 FSDataInputStream - > BufferReader
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        // 一行一行读取小表内容，并将读取的数据存入map集合

        String line = null;
        while ((line = bufferedReader.readLine()) != null){
            String[] split = line.split(",");
            map.put(split[0],line);
        }

        // 关闭流
        bufferedReader.close();
        fileSystem.close();
    }

    // map方法：收到一次数据执行一次方法
    //第二件事情：对大表的处理业务逻辑，而且实现大表和小表的 join操作
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 从行文本数据获取商品id，得到 K2
        String[] split = value.toString().split(",");
        String productId = split[2]; // K2

        // 在Map集合中，将商品的id作为键，获取值，进行拼接，得到 V2
        String productLine = map.get(productId);
        String valueLine = productLine + "\t" + value.toString(); // V2

        context.write(new Text(productId),new Text(valueLine));
    }
}
