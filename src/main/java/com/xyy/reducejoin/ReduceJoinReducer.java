package com.xyy.reducejoin;

import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.util.BeanUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class ReduceJoinReducer extends Reducer<Text,JoinBean,JoinBean,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
        List<JoinBean> list = Lists.newArrayList();

        JoinBean pd = new JoinBean();


        for(JoinBean bean: values){
            JoinBean order = new JoinBean();
            if(bean.getFlag().equals("order")){

                try {
                    BeanUtils.copyProperties(order,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                list.add(order);
            }else{

                try {
                    BeanUtils.copyProperties(pd,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }

        for (JoinBean bean : list){

            bean.setPname(pd.getPname());
            context.write(bean,NullWritable.get());
        }


    }
}
