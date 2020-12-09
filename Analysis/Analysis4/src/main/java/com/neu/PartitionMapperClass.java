/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author urvashijain
 */
public class PartitionMapperClass extends Mapper<LongWritable, Text, Text, Text>{
    private Text outKey = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        String month_year = null;
        
        if(!tokens[1].equals("date") && tokens[2].equals("f")){
            String[] date = tokens[1].split("/");
            month_year = date[0] + "-20" + date[2];
            outKey.set(month_year);
            context.write(outKey, value);
        }
    }
}
