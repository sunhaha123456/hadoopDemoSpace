package hadoop.demo.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.net.URI;

/**
 * 功能：使用MR统计HDFS上的文件对应的词频
 */
public class WordCountApp {

    public static final String HDFS_PATH = "hdfs://aly:9000";

    /**
     * 功能：以本地方式，在hdfs上，输入hdfs文件数据，并将运算结果，输出到hdfs
     * 备注：该方式不走yarn
     * @throws Exception
     */
    @Test
    public void runWordCountJobByHdfs() throws Exception {
        // 设置Job执行用户
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HDFS_PATH);
        configuration.set("dfs.client.use.datanode.hostname", "true");

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountApp.class);

        // 设置Job对应的参数：设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应的参数：Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数：Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inPutPath = new Path("/wordCount/input");
        Path outPutPath = new Path("/wordCount/output");

        // 如果输出目录已经存在，则先删除
        // 注意：（1）指定用户，需要查看hdfs中文件的用户权限
        //      （2）如果任务执行失败，也会删除该目录的
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
        if(fileSystem.exists(outPutPath)) {
            fileSystem.delete(outPutPath,true);
        }

        // 设置Job对应的参数：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, inPutPath);
        FileOutputFormat.setOutputPath(job, outPutPath);

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "成功" : "失败");
    }

    /**
     * 功能：以本地方式，输入本地文件数据，并将运算结果，输出到本地
     * 备注：（1）该方式，在本地执行，则不走hdfs，也不走yarn
     *      （2）该方式，如果打jar包，发到Linux服务器上，以yarn方式执行jar，就可以直接提交Job到YARN
     * @throws Exception
     */
    @Test
    public void runWordCountJobByLocal() throws Exception {
        Configuration configuration = new Configuration();

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountApp.class);

        // 设置Job对应的参数：设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应的参数：Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数：Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置Job对应的参数：作业输入和输出的路径
        Path inPutPath = new Path("input/wc");
        Path outPutPath = new Path("output/wc");

        FileInputFormat.setInputPaths(job, inPutPath);
        FileOutputFormat.setOutputPath(job, outPutPath);

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "成功" : "失败");
    }

    /**
     * 功能：以本地方式，输入本地文件数据，并将运算结果，输出到本地
     * 备注：（1）该方式，在本地执行，则不走hdfs，也不走yarn
     *      （2）该方式，如果打jar包，发到Linux服务器上，以yarn方式执行jar，就可以直接提交Job到YARN
     *      （3）Mapper使用Combiner机制
     * @throws Exception
     */
    @Test
    public void runWordCountJobByLocalWithCombiner() throws Exception {
        Configuration configuration = new Configuration();

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountApp.class);

        // 设置Job对应的参数：设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 添加Combiner的设置
        job.setCombinerClass(WordCountReducer.class);

        // 设置Job对应的参数：Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数：Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置Job对应的参数：作业输入和输出的路径
        Path inPutPath = new Path("input/wc");
        Path outPutPath = new Path("output/wc");

        FileInputFormat.setInputPaths(job, inPutPath);
        FileOutputFormat.setOutputPath(job, outPutPath);

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "成功" : "失败");
    }
}
