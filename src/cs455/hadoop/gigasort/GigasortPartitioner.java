package cs455.hadoop.gigasort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class GigasortPartitioner extends
		Partitioner<LongWritable, NullWritable> {

	public int getPartition(LongWritable key, NullWritable value, int partitions) {
		// TODO Auto-generated method stub
		long divider = (long) ((((double) Long.MAX_VALUE) + 1) / partitions);
		long keyLong = key.get();
		int partitionNum = (int) (keyLong / divider);
		return partitionNum;
	}
}
