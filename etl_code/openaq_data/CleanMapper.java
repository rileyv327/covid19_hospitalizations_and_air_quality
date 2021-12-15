import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CleanMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//input is each line 
		String[] line = value.toString().split(",");
		//want to remove negative inputs an input as n/a
		if (key.get() != 0)
		{
			float measurement = Float.parseFloat(line[7]);
			if (measurement < 0)
			{
				line[7] = null;
			}
			String rec = String.join(",", line);
			//also have reducer check for duplicates which need to be removed
			context.write(new Text(rec), new IntWritable(1));
		}	
	}
}