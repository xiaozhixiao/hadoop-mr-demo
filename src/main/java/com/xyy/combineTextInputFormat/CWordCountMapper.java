package com.xyy.combineTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CWordCountMapper  extends Mapper<LongWritable,Text,Text,IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arr = line.split(" ");

        for(String str :arr){
            k.set(str);
            context.write(k,v);
        }
    }
}
