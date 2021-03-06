package hadoop.demo.mr.access;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 功能：自定义复杂数据类型
 * 备注：需要（1）按照Hadoop的规范，需要实现Writable接口
 *         （2）按照Hadoop的规范，需要实现write和readFields这两个方法
 */
@Data
public class Access implements Writable {

    private String phone;
    private long up;
    private long down;
    private long sum;

    public Access(){}

    public Access(String phone, long up, long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }

    @Override
    public String toString() {
        return phone + "," + up + "," + down + "," + sum;
    }
}