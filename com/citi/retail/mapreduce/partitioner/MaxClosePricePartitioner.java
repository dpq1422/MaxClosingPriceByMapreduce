package com.citi.retail.mapreduce.partitioner;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class MaxClosePricePartitioner extends Partitioner<Text, FloatWritable>{

	@Override
	public int getPartition(Text key, FloatWritable value, int numOfReduceTasks) {
		String partitionkey=key.toString();
		System.out.println("partitionkey:"+partitionkey);
		if(numOfReduceTasks==0) {
			return 0;
		}
		if(partitionkey.startsWith("B")) {
			return 0;
		}if(partitionkey.startsWith("A")) {
			return 1;
		}else {
			return 2;
		}
	}

}
