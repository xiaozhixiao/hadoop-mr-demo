package com.xyy.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopNReducer extends Reducer<FlowBean,Text,Text,FlowBean> {


    TreeMap<FlowBean,Text> flowMap = new TreeMap<>();
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text text : values){

            FlowBean flowBean = new FlowBean();
            flowBean.setUpFlow(key.getUpFlow());
            flowBean.setDownFlow(key.getDownFlow());
            flowBean.setSumFlow(key.getSumFlow());


            flowMap.put(flowBean, new Text(text));

            // 2 限制TreeMap数据量，超过10条就删除掉流量最小的一条数据
            if (flowMap.size() > 10) {
                // flowMap.remove(flowMap.firstKey());
                flowMap.remove(flowMap.lastKey());
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> it = flowMap.keySet().iterator();

        while (it.hasNext()) {

            FlowBean v = it.next();

            context.write(new Text(flowMap.get(v)), v);
        }

    }
}
