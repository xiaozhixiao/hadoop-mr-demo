package com.xyy.groupingComp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean  implements WritableComparable<OrderBean>{

    private int orderId;
    private double price;

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.orderId =  dataInput.readInt();
        this.price = dataInput.readDouble();

    }

    @Override
    public String toString() {
        return  orderId +
                "\t" + price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result = this.getOrderId()-o.getOrderId()>0?1:-1;
        if(result == 0){
            result =o.getOrderId() - this.getPrice()>0?1:-1;
        }
        return result;
    }
}
