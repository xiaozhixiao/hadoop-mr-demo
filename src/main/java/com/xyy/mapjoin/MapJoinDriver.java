package com.xyy.mapjoin;

import com.xyy.reducejoin.JoinBean;
import com.xyy.reducejoin.ReduceJoinDriver;
import com.xyy.reducejoin.ReduceJoinMapper;
import com.xyy.reducejoin.ReduceJoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class MapJoinDriver {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setMapperClass(MapJoinMapper.class);

        job.setJarByClass(MapJoinDriver.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path("D:\\mrworkspace\\mapjoin\\order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\mrworkspace\\mapjoin\\out"));

        job.addCacheFile(new URI("file:///d:/mrworkspace/mapjoin/pd/pd.txt"));
        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);

        System.exit(res? 0:1);
    }
}
