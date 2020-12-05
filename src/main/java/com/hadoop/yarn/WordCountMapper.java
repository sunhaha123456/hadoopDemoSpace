package com.hadoop.yarn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 功能：词频统计，统计相同单词的次数，输出(word，1)
 *
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *      KEYIN   ：Map任务读数据的输入数据行的offset，也就是每行数据起始位置的偏移量，Long类型
 *      VALUEIN : Map任务读数据的输入数据，其实就是一行行的字符串，String类型
 *      KEYOUT  ：Map任务输出数据key的类型，String类型
 *      VALUEOUT: Map任务输出数据的value的类型，Integer类型
 *
 * Long，String，String，Integer 是Java里面的数据类型，
 * 需要切换成Hadoop自定义类型：LongWritable，Text，Text，IntWritable
 * 更适用于序列化和反序列化
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");

        for(String word: words) {
            // 输出（hello, 1），并设置忽略大小写
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}