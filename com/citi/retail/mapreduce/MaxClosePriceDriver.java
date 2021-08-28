package com.citi.retail.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * MaxClosePrice.java
 * 
 * This is a driver program to calculate Max Close Price from stock dataset using MapReduce
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxClosePriceDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c=new Configuration();
		c.set("mapred.map.tasks", "6");
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"Stocks");
		
		j.setJarByClass(MaxClosePriceDriver.class);
		j.setMapperClass(MaxClosePriceMapper.class);
		j.setPartitionerClass(MaxClosePricePartitioner.class);
		j.setReducerClass(MaxClosePriceReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		j.setNumReduceTasks(1);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		
		System.exit(j.waitForCompletion(true)?0:1);
	}
}
