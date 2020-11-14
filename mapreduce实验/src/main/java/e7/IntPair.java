package e7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair>
{
    int first;  //第一个成员变量
    int second;  //第二个成员变量

    public void set(int left, int right)
    {
        first = left;
        second = right;
    }
    public int getFirst()
    {
        return first;
    }
    public int getSecond()
    {
        return second;
    }
    //反序列化，从流中的二进制转换成IntPair
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    //key的比较
    public int compareTo(IntPair o)
    {
        // TODO Auto-generated method stub
        if (first != o.first)
        {
            return first < o.first ? 1 : -1;
        }
        else if (second != o.second)
        {
            return second < o.second ? -1 : 1;
        }
        else
        {
            return 0;
        }
    }
    @Override
    public int hashCode()
    {
        return first * 157 + second;
    }
    @Override
    public boolean equals(Object right)
    {
        if (right == null)
            return false;
        if (this == right)
            return true;
        if (right instanceof IntPair)
        {
            IntPair r = (IntPair) right;
            return r.first == first && r.second == second;
        }
        else
        {
            return false;
        }
    }

    //序列化，将IntPair转化成使用流传送的二进制
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }
}
