package net.qyjohn.emr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SimpleRecordWritable implements WritableComparable<SimpleRecordWritable>
{
	public int year = 1;
	public String country;
	public String date;

	public SimpleRecordWritable(String country, String date, int year)
	{
		this.country = country;
		this.date = date;	
		this.year = year;
	}
		
	public SimpleRecordWritable()
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
	public int compareTo(SimpleRecordWritable o)
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