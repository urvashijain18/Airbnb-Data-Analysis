/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author urvashijain
 */
public class ReducerClass extends Reducer<Text, Text, Text, Text>{
    Text outVal = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numberOfBookings = 0;
        double totalPrice = 0;
        double avg = 0 ;

        for(Text val : values){
            String[] tokens = val.toString().split(",");
                if(tokens.length == 2){
                numberOfBookings += Integer.parseInt(tokens[0].trim());
                if(!tokens[1].contains("$")){
                    totalPrice += Double.parseDouble(tokens[1].trim());
                }
            }
        }

        avg = totalPrice/numberOfBookings;
        String finalval = numberOfBookings + "\t" + avg;
        outVal.set(finalval);
        context.write(key, outVal);
    }
}
