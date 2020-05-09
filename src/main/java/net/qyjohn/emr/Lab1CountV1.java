package net.qyjohn.emr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparator;

public class Lab1CountV1
{
	public static class RecordWritable implements WritableComparable<RecordWritable>
	{
		private String country;
		private String date;

		public RecordWritable(String country, String date)
		{
			this.country = country;
			this.date = date;	
		}
		
		public RecordWritable()
		{
			this.country = null;
			this.date = null;						
		}

		@Override
		public void readFields(DataInput in) throws IOException
		{
			country = in.readUTF();
			date = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeUTF(country);
			out.writeUTF(date);
		}
	
		@Override
		public int compareTo(RecordWritable o)
		{
			int cnt = country.compareTo(o.country);
			if (cnt != 0)
			{
				return cnt;
			}
			else
			{
				return date.compareTo(o.date);
			}
		}
		
		@Override
		public String toString()
		{
			return country + "\t" + date;
		}
		
/*		@Override
		public boolean equals(Object o)
		{
			RecordWritable r = (RecordWritable) o;
			return r.country.equals(this.country) && r.date.equals(this.date);
		}
*/
	    @Override
		public int hashCode()
		{
			return country.hashCode() + date.hashCode();
		}	
	
		public int getPartitionCode()
		{
			return country.hashCode();
		}	
	}
	
	public static class CountryPartitioner extends Partitioner <RecordWritable, IntWritable>
	{
		@Override
		public int getPartition(RecordWritable key, IntWritable value, int numberOfPartitions)
		{
			return Math.abs(key.getPartitionCode() % numberOfPartitions);
		}
	}	
		

	public static class Lab1Mapper extends Mapper<LongWritable, Text, RecordWritable, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			try
			{
				String line = value.toString();
				String fields[] = line.split("\t");
				String day  = fields[1].trim();	 // The second column is expected to be the day
				String year = fields[3].trim();	 // The 4th column is the year
				String cnt  = fields[51].trim(); // The 52th column is expected to be the country code
//				if (year.equals("2014"))
//				{
					context.write(new RecordWritable(cnt, day), one);				
//				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public static class Lab1Reducer extends Reducer<RecordWritable, IntWritable, RecordWritable, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(RecordWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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
		job.setOutputKeyClass(RecordWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
