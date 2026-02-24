package top.doe.mapreduce.demos.demo5;

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
public class PlayBean implements Writable {
    private String playId;
    private int userId;
    private long actionTime;
    private String eventId;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.playId);
        out.writeInt(this.userId);
        out.writeLong(this.actionTime);
        out.writeUTF(this.eventId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.playId = in.readUTF();
        this.userId = in.readInt();
        this.actionTime = in.readLong();
        this.eventId = in.readUTF();
    }
}
