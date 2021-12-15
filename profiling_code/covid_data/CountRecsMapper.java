
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;
import java.io.FileReader;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        
        String[] line = value.toString().split(",");
        
        if (line[0] != "hospital_pk") {
            context.write(new Text("Num records"), new IntWritable(1));
        }
    }
}
