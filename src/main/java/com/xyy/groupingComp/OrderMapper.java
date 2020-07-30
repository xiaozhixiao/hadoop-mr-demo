package com.xyy.groupingComp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

    private OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] arr = value.toString().split("\t");
        k.setOrderId(Integer.parseInt(arr[0]));
        k.setPrice(Double.parseDouble(arr[2]));

        context.write(k,NullWritable.get());


    }
}
