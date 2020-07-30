package com.xyy.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable,Text,Text,JoinBean> {

    String name;
    JoinBean bean = new JoinBean();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(name.startsWith("order")){

            String[] fields = line.split("\t");

            bean.setOrderId(Integer.parseInt(fields[0]));
            bean.setPid(fields[1]);
            bean.setNum(Integer.parseInt(fields[2]));
            bean.setPname("");
            bean.setFlag("order");

            k.set(fields[1]);
        }else{
            String[] fields = line.split("\t");
            bean.setOrderId(0);
            bean.setNum(0);
            bean.setPid(fields[0]);
            bean.setPname(fields[1]);
            bean.setFlag("pd");

            k.set(fields[0]);
        }

        context.write(k,bean);
    }
}
