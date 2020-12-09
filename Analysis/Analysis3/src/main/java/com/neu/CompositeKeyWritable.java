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
public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable>{
    private long listingId = 0L;
    private int year = 0;

    public CompositeKeyWritable() {
    }
    
    public CompositeKeyWritable(long listingId, int year) {
        this.listingId = listingId;
        this.year = year;
    }

    public long getListingId() {
        return listingId;
    }

    public void setListingId(long listingId) {
        this.listingId = listingId;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(listingId);
        out.writeInt(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        listingId = in.readLong();
        year = in.readInt();
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int result;
        if(listingId > o.getListingId()){
            result = 1;
        }else if(listingId < o.getListingId()){
            result = -1;
        }else {
            if(year > o.getYear()){
                result = 1;
            }else if(year < o.getYear()){
                result = -1;
            }else{
                result = 0;
            }
        }
        return result; 
    }   
}
