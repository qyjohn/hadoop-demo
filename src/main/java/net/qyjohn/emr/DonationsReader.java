package net.qyjohn.emr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class DonationsReader {

	public static void main(final String[] args) throws Exception {
		
		Path inputFile = new Path(args[0]);

		try (SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(inputFile))) 
		{
			System.out.println("Compressed ? " + reader.isBlockCompressed());

			Text key = new Text();
			DonationWritable value = new DonationWritable();
			int count = 0;
			while (reader.next(key, value)) {
//				System.out.println(value.toString());
				count++;
			}
			System.out.println(count + " records read.");
			
		}
		
	}
}
