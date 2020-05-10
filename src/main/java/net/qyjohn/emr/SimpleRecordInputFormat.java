package net.qyjohn.emr;

import java.io.IOException;
import java.security.MessageDigest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class SimpleRecordInputFormat extends FileInputFormat<Text, SimpleRecordWritable>
{
	public RecordReader<Text, SimpleRecordWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		return new SimpleRecordInputFormatReader();
	}

	public class SimpleRecordInputFormatReader extends RecordReader<Text, SimpleRecordWritable>
	{
		private LineRecordReader lineRecordReader = null;
		private Text key = null;
		private SimpleRecordWritable value = null;

		@Override
		public void close() throws IOException
		{
			if (null != lineRecordReader)
			{
				lineRecordReader.close();
				lineRecordReader = null;
			}
			key = null;
			value = null;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException
		{
			return key;
		}

		@Override
		public SimpleRecordWritable getCurrentValue() throws IOException, InterruptedException
		{
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException
		{
			return lineRecordReader.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
		{
			close();
			lineRecordReader = new LineRecordReader();
			lineRecordReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException
		{
			boolean notEOF = true;
			boolean searchNext = true;
			
			while (notEOF && searchNext)
			{
				if (!lineRecordReader.nextKeyValue())
				{
					notEOF = false;
					key = null;
					value = null;
					return false;
				}
				
				try
				{
		            String line = lineRecordReader.getCurrentValue().toString();
					String fields[] = line.split("\t");
					String dayField = fields[1].trim(); // The 2nd colume is the day
					// convert from YYYYMMDD to YYYY-MM-DD
					String day  = String.format("%s-%s-%s", dayField.substring(0,4), dayField.substring(4,6),dayField.substring(6,8));
					int year = Integer.parseInt(fields[3].trim());	 // The 4th column is the year
					String cnt  = fields[51].trim(); // The 52th column is expected to be the country code

					// Using the MD5 as the key for the mapper. This will not be used anyway.
					MessageDigest md = MessageDigest.getInstance("MD5");
					md.update(line.getBytes());
					String digest = md.digest().toString();
		            key = new Text(digest);
		            value = new SimpleRecordWritable(cnt, day, year);
		            
		            searchNext = false;
		            return true;
				} catch (Exception e){}
			}
			
			// By default, if the above-mentioned logic fails, everything should fail
			return false;
        }

    }
}    
    