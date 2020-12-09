/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author urvashijain
 */
public class SortingReducerClass extends Reducer<CompositeKeyClass, DoubleWritable, Text, DoubleWritable>{
   
    @Override
    protected void reduce(CompositeKeyClass key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for(DoubleWritable val : values){
            Text outKey = new Text();
            outKey.set(key.getMonth() + "\t" + key.getNumOfBookings());
            context.write(outKey, val);
        }
    }
}
