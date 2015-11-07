import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class CrimeCountDriver{
	
	public static int main(String[] args)
	{
		int exitCode=0;

		if (args.length !=3)
		{
			System.err.println("Usage: CrimeCountDriver <input path> <output path> <region digits>");
			System.exit(-1);
		}

		try
		{	
			
			// get the default configuration object
			Configuration conf = new Configuration ();

			// get the number of digits for the region
			conf.setInt("regionDigit", Integer.parseInt(args[2]));
			
			// create the MapReduce job
			Job job;

			job = new Job (conf);

			job.setJobName ("CrimeCount");

			// output text/int pairs (since we have words as keys and counts as values)
			job.setMapOutputKeyClass (Text.class);
			job.setMapOutputValueClass (IntWritable.class);

			// output text/int pairs (since we have words as keys and counts as values)
			job.setOutputKeyClass (Text.class);
			job.setOutputValueClass (IntWritable.class);  

			// tell Hadoop the mapper and the reducer to use
			//job.setMapperClass (CrimeCountMapper.class);
			job.setMapperClass (CrimeCountMapper.class);
			job.setReducerClass (CrimeCountReducer.class);

			// reading in a text file, so we can use Hadoop's built-in TextInputFormat
			job.setInputFormatClass(TextInputFormat.class);

			// use Hadoop's built-in TextOutputFormat for writing out the output text file
			job.setOutputFormatClass (TextOutputFormat.class);  

			// set the input and output paths
			TextInputFormat.setInputPaths (job, new Path (args[0]));
			TextOutputFormat.setOutputPath (job, new Path (args[1]));

			// this tells Hadoop to ship around the jar file containing "CrimeCountDriver.class" to all of the different
			// nodes so that they can run the job
			job.setJarByClass(CrimeCountDriver.class);

			// submit the job and wait for it to complete
			exitCode = job.waitForCompletion (true) ? 0 : 1;
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException ie)
		{
			ie.printStackTrace();
		}
		catch (ClassNotFoundException ce)
		{
			ce.printStackTrace();
		}
		return exitCode;
	}
}
