package hadoop.demo.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MapReduce自定义分区规则
 * Partitioner决定maptask输出的数据交由哪个reducetask处理，可以用于定义输出数据文件的数量
 */
public class AccessPartitioner extends Partitioner<Text, Access>{

    /**
     * @param phone 手机号
     */
    @Override
    public int getPartition(Text phone, Access access, int numReduceTasks) {

        if(phone.toString().startsWith("13")) {
            return 0;
        } else if(phone.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
