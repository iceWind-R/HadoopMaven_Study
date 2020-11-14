package e7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<IntPair, IntWritable> {
    @Override
    public int getPartition(IntPair intPair, IntWritable intWritable, int i) {
        return Math.abs(intPair.getFirst() * 127) % i;
    }
}
