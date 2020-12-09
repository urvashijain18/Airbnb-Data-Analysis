/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author urvashijain
 */
public class ReducerClass extends Reducer<CompositeKeyWritable, IntWritable, Text, IntWritable>{
    IntWritable value = new IntWritable();
    Text outKey = new Text();
    
    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalCount = 0;
        for(IntWritable val : values){
            totalCount += val.get();
        }
        
        value.set(totalCount);
        outKey.set(key.getListingId() + "\t" + key.getYear());
        context.write(outKey, value);
    }
}
