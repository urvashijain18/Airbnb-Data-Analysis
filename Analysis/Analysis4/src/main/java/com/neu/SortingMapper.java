/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author urvashijain
 */
public class SortingMapper extends Mapper<LongWritable, Text, CompositeKeyClass, DoubleWritable> {
    DoubleWritable outVal = new DoubleWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");
        //String[] vals = tokens[0].split("\\s+");
        
        if (tokens.length == 3) {
            String numOfBookings = tokens[1].trim();
            outVal.set(Double.parseDouble(tokens[2].trim()));
            CompositeKeyClass obj = new CompositeKeyClass(tokens[0].trim(), numOfBookings);
            context.write(obj, outVal);
        }
    }
}
