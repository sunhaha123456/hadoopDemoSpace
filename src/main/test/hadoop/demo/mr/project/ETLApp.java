package hadoop.demo.mr.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 功能：ETL 处理
 * 备注：只有Mapper，无Reduce
 */
public class ETLApp {

    /**
     * 功能：以本地方式，输入本地文件数据，并将运算结果，输出到本地
     * 备注：（1）该方式，在本地执行，则不走hdfs，也不走yarn
     *      （2）该方式，如果打jar包，发到Linux服务器上，以yarn方式执行jar，就可以直接提交Job到YARN
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        // 如果输出目录已经存在，则先删除
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("input/wc/etl");
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath,true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ETLApp.class);

        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("input/wc/wc.txt"));
        FileOutputFormat.setOutputPath(job, new Path("input/wc/etl"));

        boolean res = job.waitForCompletion(true);

        System.out.println(res ? "SUCCESS" : "FAIL");
    }

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            String[] arr = log.split("\t");

            StringBuilder builder = new StringBuilder();
            builder.append(arr[0]).append("\t").append(arr[1]);

            context.write(NullWritable.get(), new Text(builder.toString()));
        }
    }
}