/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author urvashijain
 */
public class CompositeKeyComparator extends WritableComparator {

    public CompositeKeyComparator(){
        super(CompositeKeyClass.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyClass ckw1 = (CompositeKeyClass) a;
        CompositeKeyClass ckw2 = (CompositeKeyClass) b;

        int result = -1 * ckw1.getNumOfBookings().compareTo(ckw2.getNumOfBookings());
        return result;
    }      
}
