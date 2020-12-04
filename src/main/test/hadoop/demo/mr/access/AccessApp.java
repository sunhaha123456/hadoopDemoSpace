package hadoop.demo.mr.access;

import hadoop.demo.mr.wc.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class AccessApp {

    // 功能：以本地方式，无指定Part，即使用默认Part的方式，执行任务
    @Test
    public void runAccessJobByLocalWithoutPart() throws Exception{
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        // 设置Job对应的参数：作业输入和输出的路径
        Path inputPath = new Path("input/access");
        Path outputPath = new Path("output/access");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "成功" : "失败");
    }

    // 功能：以本地方式，使用指定Part，执行任务
    @Test
    public void runAccessJobByLocalWithPart() throws Exception{
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // 设置自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);
        // 设置reduce个数
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        // 设置Job对应的参数：作业输入和输出的路径
        Path inputPath = new Path("input/access");
        Path outputPath = new Path("output/access");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "成功" : "失败");
    }
}