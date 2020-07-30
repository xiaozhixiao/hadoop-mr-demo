package com.xyy.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanCompMapper extends Mapper<LongWritable,Text,FlowBeanComparable,Text> {

    private  FlowBeanComparable k = new FlowBeanComparable();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] arr = s.split("\t");

        k.setUpFlow(Long.parseLong(arr[1]));
        k.setDownFlow(Long.parseLong(arr[2]));
        k.setSumFlow(Long.parseLong(arr[3]));

        v.set(arr[0]);
        context.write(k,v);
    }
}
