package com.xyy.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartition extends Partitioner<FlowBeanComparable,Text> {
    @Override
    public int getPartition(FlowBeanComparable flowBeanComparable, Text text, int i) {
        int partion = 4;
        String phoneNum = text.toString().substring(0, 3);
        if("136".equals(phoneNum)){
            partion = 0;
        }else if("137".equals(phoneNum)){
            partion = 1;
        }else if("138".equals(phoneNum)){
            partion = 2;
        }else if("139".equals(phoneNum)){
            partion = 3;
        }else{
            partion = 4;
        }
        return partion;
    }
}
