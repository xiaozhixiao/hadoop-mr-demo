package com.xyy.customOutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

    private Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String s = key.toString();

        s = s + "\r\n";

        k.set(s);
        context.write(k,NullWritable.get());

    }
}
