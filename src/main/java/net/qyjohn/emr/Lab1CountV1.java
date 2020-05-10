package net.qyjohn.emr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class Lab1CountV1
{
	public static class CountryPartitioner extends Partitioner <SimpleRecordWritable, IntWritable>
	{
		@Override
		public int getPartition(SimpleRecordWritable key, IntWritable value, int numberOfPartitions)
		{
			return Math.abs(key.getPartitionCode() % numberOfPartitions);
		}
	}	
    
	public static class Lab1Mapper extends Mapper<Text, SimpleRecordWritable, SimpleRecordWritable, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);

		public void map(Text key, SimpleRecordWritable value, Context context) throws IOException, InterruptedException
		{
			if ((value.year == 2015) || (value.year == 2016))
			{
				context.write(value, one);
			}
		}
	}

	public static class Lab1Reducer extends Reducer<SimpleRecordWritable, IntWritable, SimpleRecordWritable, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(SimpleRecordWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
   		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"EMR Lab 1");
		job.setJarByClass(Lab1CountV1.class);
		job.setMapperClass(Lab1Mapper.class);
		job.setCombinerClass(Lab1Reducer.class);
		job.setPartitionerClass(CountryPartitioner.class);
		job.setReducerClass(Lab1Reducer.class);
		job.setOutputKeyClass(SimpleRecordWritable.class);
		job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SimpleRecordInputFormat.class);
        job.setOutputFormatClass(SimpleRecordOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
