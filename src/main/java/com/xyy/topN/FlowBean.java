package com.xyy.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        int result;

        if (this.sumFlow > o.getSumFlow()) {
            result = -1;
        }else if (this.sumFlow < o.getSumFlow()) {
            result = 1;
        }else {
            result = 0;
        }

        return result;

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {


        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();


    }
}
