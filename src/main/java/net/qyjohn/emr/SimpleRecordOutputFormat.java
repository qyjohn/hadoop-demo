package net.qyjohn.emr;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class SimpleRecordOutputFormat extends FileOutputFormat<SimpleRecordWritable, IntWritable>
{
	protected static class SimpleRecordWriter extends RecordWriter<SimpleRecordWritable, IntWritable>
	{
		AvroParquetWriter writer;
		String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\","
		+ "\"namespace\":\"net.qyjohn.emr\",\n" + " \"fields\":[\n"
		+ "    {\"name\":\"country\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"date\",\"type\":\"string\"},\n"
		+ "    {\"name\":\"count\",\"type\":\"int\"}\n"
		+ "]}";		
		Schema schema = Schema.parse(SCHEMA);

		public SimpleRecordWriter(Path file) throws IOException
		{
			writer = new AvroParquetWriter(file, schema, CompressionCodecName.GZIP, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
		}

		public synchronized void write(SimpleRecordWritable key, IntWritable value) throws IOException
		{
			GenericData.Record record = new GenericData.Record(schema);
			record.put("country", key.country);
			record.put("date", key.date);
			record.put("count", value.get());
			writer.write(record);
		}

		public synchronized void close(TaskAttemptContext job) throws IOException
		{
			writer.close();
		}
	}
	
	public RecordWriter<SimpleRecordWritable, IntWritable> getRecordWriter(TaskAttemptContext job) throws IOException
	{
		String file_extension = ".parquet";
		Path file = getDefaultWorkFile(job, file_extension);
		return new SimpleRecordWriter(file);
	}
}