/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author urvashijain
 */
public class DriverClass {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
	Job job = new Job(conf, "ChainingAnalysis");
	job.setJarByClass(DriverClass.class);
        
        // MapReduce chaining
        Configuration mapConf1 = new Configuration(false);
        ChainMapper.addMapper(job, PartitionMapperClass.class, LongWritable.class, Text.class, Text.class, Text.class, mapConf1);
        
        job.setPartitionerClass(PartitionerClass.class);
	PartitionerClass.setMinLastAccessDate(job, 2019);

        Configuration mapConf2 = new Configuration(false);
        ChainMapper.addMapper(job, AverageAndCountMapper.class, Text.class, Text.class, Text.class, Text.class, mapConf2);
        
        Configuration reduceConf = new Configuration(false);        
        ChainReducer.setReducer(job, ReducerClass.class, Text.class, Text.class, Text.class, Text.class, reduceConf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        boolean result = job.waitForCompletion(true);
	if(result){
            Job job1= new Job(conf, "SecondarySortAnalysis");
            job1.setJarByClass(DriverClass.class);
            
            job1.setMapperClass(SortingMapper.class);
	    job1.setReducerClass(SortingReducerClass.class);
            
            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);
                
            job1.setMapOutputKeyClass(CompositeKeyClass.class);
            job1.setMapOutputValueClass(DoubleWritable.class);
        
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);
            
            FileInputFormat.addInputPath(job1, new Path(args[1]));
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            
            job1.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
            job1.setSortComparatorClass(CompositeKeyComparator.class);
            job1.setPartitionerClass(NaturalKeyPartitioner.class);
            
            job1.setNumReduceTasks(1); 
            result = job1.waitForCompletion(true);
        }
    }
}
