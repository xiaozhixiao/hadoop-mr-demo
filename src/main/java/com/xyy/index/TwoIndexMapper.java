package com.xyy.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("-->");

        k.set(split[0]);

        String[] split1 = split[1].split("\t");

        v.set(split1[0]+"-->"+split1[1]);


        context.write(k,v);
    }
}
