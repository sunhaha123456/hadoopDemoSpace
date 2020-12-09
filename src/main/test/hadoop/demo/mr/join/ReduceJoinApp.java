package hadoop.demo.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 功能：实现Reduce端Join
 */
public class ReduceJoinApp {

    /**
     * 功能：以本地方式，输入本地文件数据，并将运算结果，输出到本地
     * 备注：（1）该方式，在本地执行，则不走hdfs，也不走yarn
     *      （2）该方式，如果打jar包，发到Linux服务器上，以yarn方式执行jar，就可以直接提交Job到YARN
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ReduceJoinApp.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataInfo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        MultipleInputs.addInputPath(job, new Path("input/join/emp"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("input/join/dept"), TextInputFormat.class);

        Path outputDir = new Path("output/join/reduce");
        outputDir.getFileSystem(configuration).delete(outputDir,true);
        FileOutputFormat.setOutputPath(job, outputDir);

        boolean res = job.waitForCompletion(true);

        System.out.println(res ? "成功！" : "失败！");
    }

    /**
     * 功能：Mapper<行开始坐标，行内容，员工部门编号，员工信息>
     */
    static class MyMapper extends Mapper<LongWritable, Text, IntWritable, DataInfo> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");
            int length = splits.length;

            StringBuilder builder = new StringBuilder();

            if (length == 5) { //emp
                String empNo = splits[0];
                String empName = splits[1];
                String empSal = splits[2];
                int deptNo = Integer.parseInt(splits[3]);

                DataInfo dataInfo = new DataInfo();
                dataInfo.setFlag("emp");
                builder.append(empNo).append("\t")
                        .append(empName).append("\t")
                        .append(empSal).append("\t");
                dataInfo.setData(builder.toString());

                context.write(new IntWritable(deptNo), dataInfo);
            } else if (length == 3) { //dept
                int deptNo = Integer.parseInt(splits[0]);
                String deptName = splits[1];

                DataInfo dataInfo = new DataInfo();
                dataInfo.setData(deptName);
                dataInfo.setFlag("dept");

                context.write(new IntWritable(deptNo), dataInfo);
            }
        }
    }

    static class MyReducer extends Reducer<IntWritable, DataInfo, Text, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<DataInfo> values, Context context) throws IOException, InterruptedException {

            List<String> emps = new ArrayList<String>();
            List<String> depts = new ArrayList<String>();

            for(DataInfo dataInfo: values) {
                if("dept".equals(dataInfo.getFlag())) { //dept
                    depts.add(dataInfo.getData());
                } else if("emp".equals(dataInfo.getFlag())) { //emp
                    emps.add(dataInfo.getData());
                }
            }

            //遍历两个List
            int i,j;

            for(i=0; i<emps.size(); i++) {
                for(j=0; j<depts.size();j++) {
                    context.write(new Text(emps.get(i) + "\t" + depts.get(j)), NullWritable.get());
                }
            }
        }
    }
}