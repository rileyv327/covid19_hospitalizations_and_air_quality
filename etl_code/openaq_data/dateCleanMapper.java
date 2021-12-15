import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class dateCleanMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//input is each line 
		String[] line = value.toString().split(",");

		String dateTime = line[5];	
		//String[] dT = datetime.toString().split();
		//String[] n = new String[10];
		String n = "";
		for (int i = 0; i < 10; i++)
		{
			if (i < 10)
			{
				n = n + dateTime.charAt(i); //only take first 10 values
			}
		}
		//String date = String.join("", n);
		line[5] = n;

		//take out the other date/time column -- not needed anymore
		String[] cleaned = new String[9];
		for (int i = 0; i < 10; i++)
		{
			if (i < 4)
			{
				cleaned[i] = line[i];
			}
			else if (i > 4)
			{
				cleaned[i-1] = line[i];
			}
		}

		String rec = String.join(",", cleaned);

		context.write(new Text(rec), new IntWritable(1));
	}
}
