package com.hadoop.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 使用MR统计HDFS上的文件对应的词频
 *
 * Driver: 配置Mapper，Reducer的相关属性
 *
 * 以提交到yarn的方式，执行Job
 */
public class WordCountAppYarn {

    public static final String HDFS_PATH = "hdfs://hadoop000:9000";

    public static void main(String[] args) throws Exception {
        boolean res = runWordCountJob();
        System.out.println(res ? "SUCCESS" : "FAIL");
    }

    // 功能：在hdfs上，输入hdfs文件数据，并将运算结果，输出到hdfs
    public static boolean runWordCountJob() throws Exception {
        // 设置Job执行用户
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HDFS_PATH);
        configuration.set("dfs.client.use.datanode.hostname", "true");

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountAppYarn.class);

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

        return result;
    }
}