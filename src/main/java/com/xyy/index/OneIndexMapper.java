package com.xyy.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    String name;

    private Text k = new Text();
    IntWritable v = new IntWritable();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(" ");

        for(String s : split){
            String retKey = s + "-->"+name;
            k.set(retKey);
            v.set(1);
            context.write(k,v);
        }

    }
}
