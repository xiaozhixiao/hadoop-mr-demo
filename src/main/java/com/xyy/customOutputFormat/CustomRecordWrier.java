package com.xyy.customOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CustomRecordWrier extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream aOut = null;
    FSDataOutputStream bOut = null;
    public CustomRecordWrier(TaskAttemptContext context)  {

        Configuration configuration = context.getConfiguration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
            aOut= fs.create(new Path("D:\\mrworkspace\\a.log"));
            bOut= fs.create(new Path("D:\\mrworkspace\\b.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        String s = text.toString();
        if(s.contains("a")){
            aOut.write(s.getBytes());
        }else {
            bOut.write(s.getBytes());
        }


    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(aOut);
        IOUtils.closeStream(bOut);
    }
}
