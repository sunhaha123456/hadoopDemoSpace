package hadoop.demo.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 功能：实现Map端Join
 * 备注：只能用于本地，不能提交到YARN，因为读取input文件，是使用的本地流的方式
 */
public class MapJoinByLocalApp {

    public static void main(String[] args)throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MapJoinByLocalApp.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);  //设置没有reduce

        //把小文件加到分布式缓存
        //注意：此处最好是传文件路径，而不是传目录路径，如果传目录路径，需要特殊处理
        job.addCacheFile(new URI("input/join/dept/dept.txt"));
        FileInputFormat.setInputPaths(job, new Path("input/join/emp"));

        Path outputDir = new Path("output/join/map");
        outputDir.getFileSystem(configuration).delete(outputDir,true);
        FileOutputFormat.setOutputPath(job, outputDir);

        boolean res = job.waitForCompletion(true);
        System.out.println(res ? "成功！" : "失败！");
    }

    static class MyMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

        private static Map<Integer,String> cache = new ConcurrentHashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String path = context.getCacheFiles()[0].toString();
            BufferedReader reader = new BufferedReader(new FileReader(path));

            String readLine = null;

            while((readLine = reader.readLine()) != null) {
                String[] splits = readLine.split("\t");  // dept
                int deptno = Integer.parseInt(splits[0]);
                String dname = splits[1];

                cache.put(deptno, dname);
            }
        }

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

                String deptName = cache.get(deptNo);

                builder.append(empNo).append("\t")
                        .append(empName).append("\t")
                        .append(empSal).append("\t")
                        .append(deptName);

                context.write(new Text(builder.toString()), NullWritable.get());
            }
        }
    }
}