package com.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * 功能：使用MR统计HDFS上的文件对应的词频
 *      以提交到yarn的方式，执行Job
 * 备注：start.sh 中，是Linux 上的启动脚本
 */
public class WordCountAppYarn {

    private static final Logger logger = LoggerFactory.getLogger(WordCountAppYarn.class);

    public static void main(String[] args) {
        WordCountAppYarn wc = new WordCountAppYarn();
        wc.runWordCountJob(args[0], args[1], args[2]);
    }

    /**
     * 功能：在hdfs上，输入hdfs文件数据，并将运算结果，输出到hdfs
     * @param nameNodeURI name node uri
     * @param inputPath  输入目录
     * @param outputPath 输出目录
     */
    public void runWordCountJob(String nameNodeURI, String inputPath, String outputPath) {
        try {
            // 设置Job执行用户
            //System.setProperty("HADOOP_USER_NAME", "root");

            Configuration configuration = new Configuration();
            //configuration.set("fs.defaultFS", HDFS_PATH);
            //configuration.set("dfs.client.use.datanode.hostname", "true");

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

            Path inPutPath = new Path(inputPath);
            Path outPutPath = new Path(outputPath);

            // 如果输出目录已经存在，则先删除
            // 注意：（1）指定用户，需要查看hdfs中文件的用户权限
            //      （2）如果任务执行失败，也会删除该目录的
            FileSystem fileSystem = FileSystem.get(new URI(nameNodeURI), configuration, "root");
            if(fileSystem.exists(outPutPath)) {
                fileSystem.delete(outPutPath,true);
            }

            // 设置Job对应的参数：作业输入和输出的路径
            FileInputFormat.setInputPaths(job, inPutPath);
            FileOutputFormat.setOutputPath(job, outPutPath);

            // 提交job
            boolean result = job.waitForCompletion(true);

            if (result) {
                logger.info("WordCountJob 执行成功！");
            } else {
                logger.info("WordCountJob 执行失败！");
            }
        } catch (Exception e) {
            logger.error("WordCountJob 执行异常，报错信息：{}", e);
        }
    }
}