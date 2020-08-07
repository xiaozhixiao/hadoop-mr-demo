package com.xyy.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer  extends Reducer<Text,Text,Text,Text>{
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for(Text text : values){
            sb.append(text+"\t");
        }
        v.set(sb.toString());

        context.write(key,v);
    }
}
