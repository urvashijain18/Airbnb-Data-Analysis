/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author urvashijain
 */
public class PartitionerClass extends Partitioner<Text, Text> implements Configurable {

	private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

	private Configuration conf = null;
	private int minLastAccessDateYear = 0;

        @Override
	public int getPartition(Text key, Text value, int numPartitions) {
                String[] strKey = key.toString().split("-");
                int result = Integer.parseInt(strKey[1]) - minLastAccessDateYear;
		return result;
	}

        @Override
	public Configuration getConf() {
		return conf;
	}

        @Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
	}

	public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {
		job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
	}
    
    
}
