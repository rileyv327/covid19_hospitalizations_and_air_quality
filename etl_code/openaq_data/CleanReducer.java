import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		int total = 0;
		for (IntWritable v : values)
		{
			total = Integer.sum(total, v.get());
		}
		context.write(key, new IntWritable(total)); //if duplicates, only writes it once

	}
}
