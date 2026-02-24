package top.doe.mapreduce.demos.demo2;


// {"order_id":"o1","member_id":1001,"amount":120.8,"receive_address":"北京","date":"2024-08-01"}

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order implements WritableComparable<Order> {

    private String order_id;
    private int member_id;
    private double amount;
    private String receive_address;
    private String date;

    @Override
    public void write(DataOutput out) throws IOException {

        // mapTask 拿到我们map方法输出的order对象，来调write方法得到字节数组
        out.writeUTF(this.order_id);
        out.writeInt(this.member_id);
        out.writeDouble(this.amount);
        out.writeUTF(this.receive_address);
        out.writeUTF(this.date);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        //   reduceTask拿到字节，反射一个order空对象
        //   然后调用order.readFields()来恢复数据 ，然后传给  reduce()方法
        //   cls = class.forName("top.doe.mapreduce.demos.demo2.Order)
        //   order = cls.newInstance()
        //   order.readFields(in)


        this.order_id = in.readUTF();
        this.member_id = in.readInt();
        this.amount = in.readDouble();
        this.receive_address = in.readUTF();
        this.date = in.readUTF();


    }

    @Override
    public int compareTo(Order other) {
        return Double.compare(other.amount, this.amount);
    }
}
