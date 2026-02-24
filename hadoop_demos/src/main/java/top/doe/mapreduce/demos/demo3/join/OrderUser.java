package top.doe.mapreduce.demos.demo3.join;


// {"order_id":"o1","member_id":1001,"amount":120.8,
// "receive_address":"beijing","date":"2024-08-1"}

// {"id":1001,"member_level":2,"gender":"male"}


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
public class OrderUser implements Writable {

    private String tableName;

    private String order_id;
    private Integer member_id;
    private double amount;
    private String receive_address;
    private String date;
    private Integer id;
    private int member_level;
    private String gender;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.tableName);

        if (this.tableName.equals("order")) {

            out.writeUTF(this.order_id);
            out.writeInt(this.member_id);
            out.writeDouble(this.amount);
            out.writeUTF(this.receive_address);
            out.writeUTF(this.date);
        } else if(this.tableName.equals("user")){

            out.writeInt(this.id);
            out.writeInt(this.member_level);
            out.writeUTF(this.gender);
        }else{
            out.writeUTF(this.order_id);
            out.writeInt(this.member_id);
            out.writeDouble(this.amount);
            out.writeUTF(this.receive_address);
            out.writeUTF(this.date);

            out.writeInt(this.id);
            out.writeInt(this.member_level);
            out.writeUTF(this.gender);


        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tableName = in.readUTF();

        if (this.tableName.equals("order")) {
            this.order_id = in.readUTF();
            this.member_id = in.readInt();
            this.amount = in.readDouble();
            this.receive_address = in.readUTF();
            this.date = in.readUTF();
        } else if(this.tableName.equals("user")){
            this.id = in.readInt();
            this.member_level = in.readInt();
            this.gender = in.readUTF();
        }else{
            this.order_id = in.readUTF();
            this.member_id = in.readInt();
            this.amount = in.readDouble();
            this.receive_address = in.readUTF();
            this.date = in.readUTF();
            this.id = in.readInt();
            this.member_level = in.readInt();
            this.gender = in.readUTF();

        }

    }
}
