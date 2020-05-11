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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class Lab1CountV2
{

	
	public static class CountryPartitioner extends Partitioner <SimpleRecordWritable, IntWritable>
	{
		@Override
		public int getPartition(SimpleRecordWritable key, IntWritable value, int numberOfPartitions)
		{
			return Math.abs(key.getPartitionCode() % numberOfPartitions);
		}
	}	
	
    
	public static class Lab1Mapper extends Mapper<Object, Text, SimpleRecordWritable, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			try
			{
				String line = value.toString();
				String fields[] = line.split("\t");
				String dayField = fields[1].trim(); // The 2nd colume is the day
				// convert from YYYYMMDD to YYYY-MM-DD
				String day  = String.format("%s-%s-%s", dayField.substring(0,4), dayField.substring(4,6),dayField.substring(6,8));
				int year = Integer.parseInt(fields[3].trim());	 // The 4th column is the year
				String cnt  = fields[51].trim(); // The 52th column is expected to be the country code

				if ((year == 2015) || (year == 2016))
				{
					context.write(new SimpleRecordWritable(cnt, day, year), one);
				}
			} catch (Exception e) {}
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
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
    		
		Job job = new Job(conf,"EMR Lab 1");
		job.setJarByClass(Lab1CountV1.class);
		job.setMapperClass(Lab1Mapper.class);
		job.setCombinerClass(Lab1Reducer.class);
		job.setPartitionerClass(CountryPartitioner.class);
		job.setReducerClass(Lab1Reducer.class);
		job.setOutputKeyClass(SimpleRecordWritable.class);
		job.setOutputValueClass(IntWritable.class);
	        job.setOutputFormatClass(SimpleRecordOutputFormat.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
        	CombineTextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
