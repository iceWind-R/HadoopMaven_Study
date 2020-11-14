package e7;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    protected GroupingComparator() {
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair ip1 = (IntPair) a;
        IntPair ip2 = (IntPair) b;
        int l = ip1.getFirst();
        int r = ip2.getFirst();
        return l == r ? 0 : (l < r ? -1 : 1);

    }
}
