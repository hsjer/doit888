package top.doe.mapreduce.demos.demo6;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {

    private String order_id;
    private int member_id;
    private double amount;
    private String receive_address;
    private String date;

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.order_id);
        out.writeInt(this.member_id);
        out.writeDouble(this.amount);
        out.writeUTF(this.receive_address);
        out.writeUTF(this.date);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.order_id = in.readUTF();
        this.member_id = in.readInt();
        this.amount = in.readDouble();
        this.receive_address = in.readUTF();
        this.date = in.readUTF();
    }


    @Override
    public int compareTo(OrderBean o) {
        // 先按用户id排，再按订单金额排
        // 这样可以保证相同用户的订单排列在一起，且金额大的在前
        int uRes = Integer.compare(this.member_id, o.member_id);

        return uRes == 0 ? Double.compare(o.amount,this.amount) : uRes;
    }
}
