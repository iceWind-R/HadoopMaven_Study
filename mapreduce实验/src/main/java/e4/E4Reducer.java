package e4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class E4Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int buyernum = 0;
        String[] buyer = new String[20];
        int friendsnum = 0;
        String[] friends = new String[20];
        Iterator ite = values.iterator();
        while(ite.hasNext())

        {
            String record = ite.next().toString();
            int len = record.length();
            int i = 2;
            if (0 == len) {
                continue;
            }
            //取得左右表标识
            char relationtype = record.charAt(0);
            //取出record，放入buyer
            if ('1' == relationtype) {
                buyer[buyernum] = record.substring(i);
                buyernum++;
            }
            //	取出record，放入friends
            if ('2' == relationtype) {
                friends[friendsnum] = record.substring(i);
                friendsnum++;
            }
        }
//	    buyernum和friendsnum数组求笛卡尔积
        if(0!=buyernum&&0!=friendsnum)

        {
            for (int m = 0; m < buyernum; m++) {
                for (int n = 0; n < friendsnum; n++) {
                    if (buyer[m] != friends[n]) {
                        //输出结果
                        context.write(new Text(buyer[m]), new Text(friends[n]));
                    }
                }
            }

        }
    }
}
