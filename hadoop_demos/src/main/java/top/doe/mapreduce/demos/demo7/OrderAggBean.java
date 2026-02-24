package top.doe.mapreduce.demos.demo7;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderAggBean implements Writable {

    private String province;
    private double order_amount;
    private int order_count;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.province);
        out.writeDouble(this.order_amount);
        out.writeInt(this.order_count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.province = in.readUTF();
        this.order_amount = in.readDouble();
        this.order_count = in.readInt();
    }
}
