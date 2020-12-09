package hadoop.demo.mr.join;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class DataInfo implements Writable{

    private String data;
    private String flag;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(data);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.data = in.readUTF();
        this.flag = in.readUTF();
    }
}