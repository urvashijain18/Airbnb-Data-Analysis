/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author urvashijain
 */
public class AverageAndCountMapper extends Mapper<Text, Text, Text, Text>{
    Text outVal = new Text();
    
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    
        String line = value.toString();
        String[] tokens = line.split(",");
        
        if(!tokens[3].isEmpty()){
            outVal.set("1" + " , "+ tokens[3]);
            context.write(key, outVal); 
        }
    }
}
