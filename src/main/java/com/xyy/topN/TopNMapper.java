package com.xyy.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    FlowBean flowBean;
    Text v = new Text();
    TreeMap<FlowBean,Text> flowMap = new TreeMap<>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        flowBean = new FlowBean();
        String[] split = value.toString().split("\t");
        String phone = split[0];
        Long upFlow = Long.parseLong(split[1]);
        Long downFlow = Long.parseLong(split[2]);
        Long sumFlow = Long.parseLong(split[3]);

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(sumFlow);


        v.set(phone);
        flowMap.put(flowBean,v);

        if(flowMap.size()>10){
            flowMap.remove(flowMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iterator = flowMap.keySet().iterator();
        while(iterator.hasNext()){
            FlowBean next = iterator.next();
            context.write(next,flowMap.get(next));
        }

    }
}
