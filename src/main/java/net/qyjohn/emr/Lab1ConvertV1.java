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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.avro.Schema;
import org.apache.parquet.example.data.Group;

public class Lab1ConvertV1
{
	public static class TextToAvroParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord>
	{
		String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\","
		+ "\"namespace\":\"net.qyjohn.emr\",\n" + " \"fields\":[\n"
		+ "    {\"name\":\"country\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"date\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"count\",\"type\":\"int\"}\n"
		+ "]}";		
		Schema schema = Schema.parse(SCHEMA);
		private GenericRecord myGenericRecord = new GenericData.Record(schema);

		@Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    {
		    try
		    {
				// Parse the value yourself here,
				String line = value.toString();
				String fields[] = line.split("\t");
				String country  = fields[0].trim();	 
				String date = fields[1].trim();	 
				int count  = Integer.parseInt(fields[2].trim()); 
				myGenericRecord.put("country", country);
				myGenericRecord.put("date", date);
				myGenericRecord.put("count", count);
								
				context.write(null, myGenericRecord);			    
		    } catch (Exception e)
		    {
			    System.out.println(e.getMessage());
			    e.printStackTrace();
		    }
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"EMR Lab 1");
		job.setJarByClass(Lab1ConvertV1.class);
		job.setMapperClass(TextToAvroParquetMapper.class);
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		
		
		String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\","
		+ "\"namespace\":\"net.qyjohn.emr\",\n" + " \"fields\":[\n"
		+ "    {\"name\":\"country\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"date\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"count\",\"type\":\"int\"}\n"
		+ "]}";		
		Schema schema = Schema.parse(SCHEMA);
		AvroParquetOutputFormat.setSchema(job, schema);		
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Void.class);
		job.setOutputValueClass(Group.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
