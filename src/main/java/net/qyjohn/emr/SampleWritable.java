package net.qyjohn.emr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class SampleWritable implements WritableComparable<SampleWritable>
{
	public String donation_id;
	public String project_id;
	public String donor_city;

	@Override
	public void readFields(DataInput in) throws IOException
	{
		donation_id = in.readUTF();
		project_id = in.readUTF();
		donor_city = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(donation_id);
		out.writeUTF(project_id);
		out.writeUTF(donor_city);
	}

	public void parseLine(String line) throws IOException
	{
		String[] parts = line.split("\t");

		donation_id = parts[0];
		project_id = parts[1];
		donor_city = parts[2];
	}

	@Override
	public int compareTo(SampleWritable o) {
		return this.donation_id.compareTo(o.donation_id);
	}

	@Override
	public String toString() {
		return String.format("%s|project=%s|city=%s",
				donation_id, project_id, donor_city);
	}
}
