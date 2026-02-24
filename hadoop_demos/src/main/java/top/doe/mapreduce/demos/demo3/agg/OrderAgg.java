package top.doe.mapreduce.demos.demo3.agg;

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
public class OrderAgg implements Writable {

    private String gender;
    private double amount;
    private int count;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.gender);
        out.writeDouble(this.amount);
        out.writeInt(this.count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.gender = in.readUTF();
        this.amount = in.readDouble();
        this.count = in.readInt();

    }
}
