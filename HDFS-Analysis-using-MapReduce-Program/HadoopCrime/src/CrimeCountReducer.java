import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeCountReducer extends Reducer <Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
	{
		try 
		{
			int count = 0;

			for(IntWritable val: values)
			{
				count += val.get();
			}

			context.write(key, new IntWritable(count));
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
