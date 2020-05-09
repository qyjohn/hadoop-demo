package net.qyjohn.emr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;


public class DonationsWriter {

	public static void main(final String[] args) throws Exception {

		long start = System.currentTimeMillis();
		
		File inputFile = new File(args[0]);
		Path outputPath = new Path(args[1]);

		int processed = 0;
		int errors = 0;
		
		try 
		(
			// Buffered reader to 
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			
			// Create a SequenceFile Writer with block compression
			SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(), 
				SequenceFile.Writer.file(outputPath),
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(DonationWritable.class),
				SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()));
		) {
			
			for (String line = br.readLine(); line != null; line = br.readLine()) {
				
				// Ignore first line with column names
				if (processed == 0) {
					processed++;
					continue;
				}
				
				try {
					// Parse csv line to create DonationWritable object
					DonationWritable donation = new DonationWritable();
					donation.parseLine(line);
					
					// Append key and object to writer
					Text key = new Text(donation.donation_id);
					writer.append(key, donation);
					
				} catch (Exception e) {
					errors++;
				}

				processed++;
				if (processed % 1000000 == 0) {
					System.out.println(String.format("%d million lines processed", processed / 1000000));
				}
			}
			
		} finally {
			System.out.println("Number of lines processed : " + processed);
			System.out.println("Number of errors : " + errors);
			System.out.printf("Took %d ms.\n", System.currentTimeMillis() - start);
		}
	}

}
