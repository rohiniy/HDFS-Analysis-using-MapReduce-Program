import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	private final static IntWritable ONE =  new IntWritable(1);
	private static int regionDigit;
	
	
	public void map(LongWritable key, Text value,
			Context context)
	{	
		try 
		{
			Configuration conf = context.getConfiguration();
			regionDigit = Integer.parseInt(conf.get("regionDigit"));
			
			String line = value.toString();

			String [] tokens = line.split(",");

			if (tokens.length >= 8) 
			{
				String east = tokens[4].trim();
				String north = tokens[5].trim();

				if(!east.isEmpty() && !north.isEmpty())
				{
					String crimeLocation = "";
					String eastNewVal = "";
					String northNewVal = "";

					switch(regionDigit) {
					case 1:
						eastNewVal = east.substring(0, 1).trim();
						northNewVal = east.substring(0, 1).trim();
						break;

					case 2:
						eastNewVal = east.substring(0, 2).trim();
						northNewVal = east.substring(0, 2).trim();
						break;

					case 3:
						eastNewVal = east.substring(0, 3).trim();
						northNewVal = east.substring(0, 3).trim();
						break;

					case 4:
						eastNewVal = east.substring(0, 4).trim();
						northNewVal = east.substring(0, 4).trim();
						break;

					case 5:
						eastNewVal = east.substring(0, 5).trim();
						northNewVal = east.substring(0, 5).trim();
						break;

					}

					crimeLocation = eastNewVal + northNewVal;
					String crimetype = tokens[7].trim();
					String concatenatedKey = crimeLocation.concat(crimetype);

					if(concatenatedKey.charAt(0)!='E')
					{
						context.write(new Text(concatenatedKey),ONE);
					}
				}
			}
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
