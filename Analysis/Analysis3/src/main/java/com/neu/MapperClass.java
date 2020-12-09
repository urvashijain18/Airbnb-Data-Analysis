/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author urvashijain
 */
public class MapperClass extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable>{
    private final IntWritable count = new IntWritable(1);
    CompositeKeyWritable obj = new CompositeKeyWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
        String line = value.toString();    
        String[] tokens = line.split(",");
        
        if(tokens.length >= 6){
            String year = tokens[2].split("-")[0]; 
            if(tokens[0].matches("^[0-9]+$") && year.matches("^[0-9]+$")){
                obj.setListingId(Long.parseLong(tokens[0]));
                obj.setYear(Integer.parseInt(year));
                context.write(obj, count);
            }
        }
    }
}
