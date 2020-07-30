package com.xyy.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PFlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job  = Job.getInstance(conf);

        job.setJarByClass(PFlowDriver.class);

        job.setMapperClass(PFlowMapper.class);
        job.setReducerClass(PFlowReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setPartitionerClass(CustomPartition.class);
        job.setNumReduceTasks(5);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
