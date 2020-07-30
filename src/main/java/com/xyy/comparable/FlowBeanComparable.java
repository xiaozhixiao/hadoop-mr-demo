package com.xyy.comparable;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBeanComparable implements WritableComparable<FlowBeanComparable> {


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
    public int compareTo(FlowBeanComparable o) {
        return o.getSumFlow()>this.getSumFlow()?1 : -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.sumFlow);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }
}
