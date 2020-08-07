package com.xyy.comparable;

import com.xyy.flow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowBeanCompDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowBeanCompDriver.class);

        job.setMapperClass(FlowBeanCompMapper.class);
        job.setReducerClass(FlowBeanCompReducer.class);

        job.setMapOutputKeyClass(FlowBeanComparable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeanComparable.class);

        job.setPartitionerClass(CustomPartition.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:-1);
    }
}
