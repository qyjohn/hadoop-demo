package net.qyjohn.emr;

import java.util.LinkedList;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;

public class SimpleRecordOutputFormat extends FileOutputFormat<SimpleRecordWritable, IntWritable>
{
	protected static class SimpleRecordWriter extends RecordWriter<SimpleRecordWritable, IntWritable>
	{
		String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\","
		+ "\"namespace\":\"net.qyjohn.emr\",\n" + " \"fields\":[\n"
		+ "    {\"name\":\"country\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"date\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"count\",\"type\":\"int\"}\n"
		+ "]}";		
		Schema schema = Schema.parse(SCHEMA);
		
		LinkedList<GenericData.Record> records = new LinkedList<GenericData.Record>();
		
		private DataOutputStream out;
//		ParquetWriter<GenericData.Record> writer;
		Path file;

		public SimpleRecordWriter(DataOutputStream out) throws IOException
		{
			this.out = out;
			out.writeBytes("<Output>\n");
		}

		public SimpleRecordWriter(Path file) throws IOException
		{
			this.file = file;
		}

		private void writeStyle(String xml_tag,String tag_value) throws IOException
		{
			out.writeBytes("<"+xml_tag+">"+tag_value+"</"+xml_tag+">\n");
		}

		public synchronized void write(SimpleRecordWritable key, IntWritable value) throws IOException
		{
/*			out.writeBytes("<record>\n");
			this.writeStyle("key", key.toString());
			this.writeStyle("value", value.toString());
			out.writeBytes("</record>\n");
*/
			GenericData.Record record = new GenericData.Record(schema);
			record.put("country", key.country);
			record.put("date", key.date);
			record.put("count", value.get());
			records.add(record);
//			writer.write(record);
		}

		public synchronized void close(TaskAttemptContext job) throws IOException
		{
			try
			{
				AvroParquetWriter writer = new AvroParquetWriter(file, schema, CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
				for (GenericData.Record record : records)
				{
					writer.write(record);
				}
				writer.close();
//				out.writeBytes("</Output>\n");
			} finally 
			{
//				writer.close();
//				out.close();
			}
		}
	}
	
	public RecordWriter<SimpleRecordWritable, IntWritable> getRecordWriter(TaskAttemptContext job) throws IOException
	{
/*		String file_extension = ".xml";
		Path file = getDefaultWorkFile(job, file_extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file, false);
		return new SimpleRecordWriter(fileOut);
*/
		String file_extension = ".parquet";
		Path file = getDefaultWorkFile(job, file_extension);
//		FileSystem fs = file.getFileSystem(job.getConfiguration());
//		FSDataOutputStream fileOut = fs.create(file, false);
		return new SimpleRecordWriter(file);
	}
}