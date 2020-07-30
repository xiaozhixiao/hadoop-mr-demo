package com.xyy.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinBean implements Writable {
    private Integer orderId;
    private String pid;
    private Integer num;
    private String pname;
    private String flag;

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(num);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        orderId = dataInput.readInt();
        pid = dataInput.readUTF();
        num = dataInput.readInt();
        pname = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return  orderId +
                "\t" + pname +
                "\t" + num ;
    }
}
