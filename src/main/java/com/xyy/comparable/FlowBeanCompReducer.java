package com.xyy.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowBeanCompReducer extends Reducer<FlowBeanComparable,Text,Text,FlowBeanComparable> {

    @Override
    protected void reduce(FlowBeanComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text value :values){
            context.write(value,key);
        }
    }
}
