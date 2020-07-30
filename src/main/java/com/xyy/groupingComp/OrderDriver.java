package com.xyy.groupingComp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OrderDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(OrderMapper.class);

        job.setReducerClass(OrderReducer.class);

        job.setJarByClass(OrderDriver.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);


    }
}
