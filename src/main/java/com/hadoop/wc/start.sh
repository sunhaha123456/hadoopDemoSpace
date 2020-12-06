cd /home/soft/hadoopDemoSpace/
nohup hadoop jar ./hadoop-demo-0.0.1.RELEASE.jar com.hadoop.wc.WordCountAppYarn hdfs://hadoop000:9000 /wordCount/input /wordCount/output >> nohup.out&