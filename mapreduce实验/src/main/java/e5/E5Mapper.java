package e5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class E5Mapper extends Mapper<Object, Text, Text, Text> {
    private Map<String, String> dict = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        String fileName = context.getLocalCacheFiles()[0].getName();
        System.out.println("fileName: "+fileName);
        BufferedReader reader = new BufferedReader(new FileReader("D:/mapreduceFile/5/orders1.txt"));
        String codeandname ;
        while (null != ( codeandname = reader.readLine() ) ) {
            String str[]=codeandname.split("\t");
            dict.put(str[0], str[2]+"\t"+str[3]);
            System.out.println("dict: " + str[0] +" "+str[2]+"\t"+str[3]);
            }
        reader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] kv = value.toString().split("\t");
        if (dict.containsKey(kv[1])) {
            context.write(new Text(kv[1]), new Text(dict.get(kv[1])+"\t"+kv[2]));
            System.out.println("kv[1]: "+kv[1]);
        }
    }
}
