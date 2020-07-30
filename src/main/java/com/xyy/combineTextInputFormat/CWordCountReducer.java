package com.xyy.combineTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CWordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    int sum;

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;

        for(IntWritable num :values){
            sum += num.get();
        }
        v.set(sum);

        context.write(key,v);

    }
}
