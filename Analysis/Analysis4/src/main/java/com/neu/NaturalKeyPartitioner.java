/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author urvashijain
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKeyClass, IntWritable> {

    @Override
    public int getPartition(CompositeKeyClass key, IntWritable value, int noOfPartitions) {
        return key.getMonth().hashCode() % noOfPartitions;
    }  
}
