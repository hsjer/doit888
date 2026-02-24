package top.doe.mapreduce.demos.demo6;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MemberId_GroupingComparator extends WritableComparator {

    public MemberId_GroupingComparator(){
        super(OrderBean.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean key1 = (OrderBean) a;
        OrderBean key2 = (OrderBean) b;

        return Integer.compare(key1.getMember_id(),key2.getMember_id());
    }
}
