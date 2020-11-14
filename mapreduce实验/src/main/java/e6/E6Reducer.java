package e6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;

public class E6Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Vector<String> left  = new Vector<String>();  //用来存放左表的数据
        Vector<String> right = new Vector<String>();  //用来存放右表的数据
        //迭代集合数据
        for (Text val : values) {
            String str = val.toString();
            //将集合中的数据添加到对应的left和right中
            if (str.startsWith("1+")) {
                left.add(str.substring(2));
                }
            else if (str.startsWith("2+")) {
                right.add(str.substring(2));
                }
            }
        //获取left和right集合的长度
        int sizeL = left.size();
        int sizeR = right.size();
        //System.out.println(key + "left:"+left);
        //System.out.println(key + "right:"+right);
        //遍历两个向量将结果写进去
        for (int i = 0; i < sizeL; i++) {
            for (int j = 0; j < sizeR; j++) {
                context.write( key, new Text(  left.get(i) + "\t" + right.get(j) ) );
                //System.out.println(key + " \t" + left.get(i) + "\t" + right.get(j));
                }
            }

    }
}
