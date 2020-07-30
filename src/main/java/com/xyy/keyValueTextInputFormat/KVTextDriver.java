package com.xyy.keyValueTextInputFormat;

import com.xyy.combineTextInputFormat.CWordCountDriver;
import com.xyy.combineTextInputFormat.CWordCountMapper;
import com.xyy.combineTextInputFormat.CWordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVTextDriver {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");

        Job job = Job.getInstance(conf);

        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setJarByClass(KVTextDriver.class);


        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
