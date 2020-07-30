package com.xyy.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartition extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        String phone = key.toString();
        String phoneNum = phone.substring(0,3);
        int partiton = 4;
        if("136".equals(phoneNum)){
            partiton = 0;
        }else if("137".equals(phoneNum)){
            partiton = 1;
        }else if("138".equals(phoneNum)){
            partiton = 2;
        }else if("139".equals(phoneNum)){
            partiton = 3;
        }else{
            partiton = 4;
        }

        return partiton;
    }
}
