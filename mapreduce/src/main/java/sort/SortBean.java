package sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private int num;

    // 实现比较器，指定排序的规则
    /*
    * 规则：
    *   第一列(word)按照字典顺序进行排列
    *   第一列相同时，第二列(num)按照升序进行排序
    * */
    @Override
    public int compareTo(SortBean o) {
        /*
        * 并不是真正排序，只是一个排序规则，返回的值 > 0 ?、 = 0 ?、< 0 ?
        * */
        // 先对第一列排序：word排序
        int result = this.word.compareTo(o.word);
        // 如果第一列相同，则按照第二列进行排序
        if (result == 0){
            return this.num - o.num;
        }
        return result;
    }

    // 实现序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }

    // 实现反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.num = dataInput.readInt();
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "SortBean{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }
}
