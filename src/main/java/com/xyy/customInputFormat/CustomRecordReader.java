package com.xyy.customInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomRecordReader  extends RecordReader<Text,BytesWritable>{

    private Text k = new Text();
    private BytesWritable v = new BytesWritable();

    private FileSplit fileSplit;
    private Configuration conf;

    private boolean flag = true;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        fileSplit = (FileSplit) inputSplit;

        conf = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(flag){

            byte[]  contents = new byte[(int) fileSplit.getLength()];

            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fis = fs.open(fileSplit.getPath());


            IOUtils.readFully(fis,contents,0, (int) fileSplit.getLength());


             k.set(fileSplit.getPath().toString());
             v.set(contents,0,contents.length);

            flag =false;
            return true;
        }

        return flag;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
