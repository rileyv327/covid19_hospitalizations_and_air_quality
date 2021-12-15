
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.zip.ZipException;
import java.io.FileReader;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        List<String> output = new ArrayList<>();

        if (key.get() == 0 && value.toString().contains("hospital_pk")) {
            return;
        } else if (line.length == 106) {

            // collection week
            String[] date = line[1].split("/");
            output.add(date[0] + "-" + date[1] + "-" + date[2]);

            // state
            output.add(line[2]);

            // city
            output.add(line[6]);

            // check if numbers are negative
            Boolean correct_data = true;

            // all adult hospital inpatient beds 7 day average
            // excel column = N
            output.add(line[13]);

            if (line[13].length() > 0) {
                if (Float.parseFloat(line[13]) < 0 && Float.parseFloat(line[13]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // all adult hospital inpatient beds occupied 7 day average
            // excel column = P
            output.add(line[15]);

            if (line[15].length() > 0) {
                if (Float.parseFloat(line[15]) < 0 && Float.parseFloat(line[15]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // total adult patients hospitalized confirmed and suspected covid 7 day average
            // excel column = Q
            output.add(line[16]);

            if (line[16].length() > 0) {
                if (Float.parseFloat(line[16]) < 0 && Float.parseFloat(line[16]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // total adult patients hospitalized confirmed covid 7 day average
            // excel column = R
            output.add(line[17]);

            if (line[17].length() > 0) {
                if (Float.parseFloat(line[17]) < 0 && Float.parseFloat(line[17]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // total staffed adult icu beds 7 day average
            // excel column = W
            output.add(line[22]);

            if (line[22].length() > 0) {
                if (Float.parseFloat(line[22]) < 0 && Float.parseFloat(line[22]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // staffed adult icu bed occupancy 7 day average
            // excel column = Y
            output.add(line[24]);

            if (line[24].length() > 0) {
                if (Float.parseFloat(line[24]) < 0 && Float.parseFloat(line[24]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // staffed icu adult patients confirmed and suspected covid 7 day average
            // excel column = Z
            output.add(line[25]);

            if (line[25].length() > 0) {
                if (Float.parseFloat(line[25]) < 0 && Float.parseFloat(line[25]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // staffed icu adult patients confirmed covid 7 day average
            // excel column = AA
            output.add(line[26]);

            if (line[26].length() > 0) {
                if (Float.parseFloat(line[26]) < 0 && Float.parseFloat(line[26]) != -9999) {
                    correct_data = false;
                }
            } else {
                correct_data = false;
            }

            // is_corrected (data has been smoothed post-submission)
            // excel column = DB
            String smoothed = line[105];

            List<String> places = Arrays.asList("BRONX", "BROOKLYN", "FLUSHING", "JAMAICA", "NEW YORK", "YONKERS");

            // If data has not been smoothed
            // if (smoothed.length() == 5 && line[2].contains("NY")) {
            if (smoothed.contains("false") && line[2].contains("NY") && places.contains(line[6]) && correct_data) {

                String joined_str = StringUtils.join(output, ",");

                context.write(new Text(joined_str + ","), new IntWritable(1));
            }
        }
    }
}
