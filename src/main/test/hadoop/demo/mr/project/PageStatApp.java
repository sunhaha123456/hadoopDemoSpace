package hadoop.demo.mr.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 功能：统计各个页面的访问总次数
 * 备注：等同于 select xxx, count(1) from xxx group by xxx
 */
public class PageStatApp {

    /**
     * 功能：以本地方式，输入本地文件数据，并将运算结果，输出到本地
     * 备注：（1）该方式，在本地执行，则不走hdfs，也不走yarn
     *      （2）该方式，如果打jar包，发到Linux服务器上，以yarn方式执行jar，就可以直接提交Job到YARN
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(PageStatApp.class);

        // 设置Job对应的参数：设置自定义的Mapper和Reducer处理类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 设置Job对应的参数：Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置Job对应的参数：Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置Job对应的参数：作业输入和输出的路径
        Path inPutPath = new Path("input/wc");
        Path outPutPath = new Path("output/project/pageSate");

        FileInputFormat.setInputPaths(job, inPutPath);
        FileOutputFormat.setOutputPath(job, outPutPath);

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "成功" : "失败");
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            String[] arrs = log.split("\t");
            context.write(new Text(arrs[1]), ONE);
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;
            for (LongWritable access : values) {
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }
}