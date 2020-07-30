package com.xyy.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    FlowBean flowBean = new FlowBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");

        k.set(arr[0]);

        long upFlow = Long.parseLong(arr[1]);
        long downFlow = Long.parseLong(arr[2]);

        flowBean.set(upFlow,downFlow);
        context.write(k,flowBean);
    }
}
