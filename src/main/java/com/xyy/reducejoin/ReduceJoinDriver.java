package com.xyy.reducejoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setJarByClass(ReduceJoinDriver.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);


        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        boolean res = job.waitForCompletion(true);

        System.exit(res? 0:1);


    }
}
