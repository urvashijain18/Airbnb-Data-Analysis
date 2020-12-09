/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author urvashijain
 */
public class CompositeKeyClass implements WritableComparable<CompositeKeyClass>{
    private String month;
    private String numOfBookings;

    public CompositeKeyClass() {
    }

    public CompositeKeyClass(String month, String numOfBookings) {
        this.month = month;
        this.numOfBookings = numOfBookings;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getNumOfBookings() {
        return numOfBookings;
    }

    public void setNumOfBookings(String numOfBookings) {
        this.numOfBookings = numOfBookings;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(month);
        out.writeUTF(numOfBookings);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        month = in.readUTF();
        numOfBookings = in.readUTF();
    }

    @Override
    public int compareTo(CompositeKeyClass obj) {
        int result = this.numOfBookings.compareTo(obj.numOfBookings);
        return (result < 0 ? -1 : (result == 0 ? 0 : 1));
    }   
}
