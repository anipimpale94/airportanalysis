import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancelReasonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

    String record = value.toString();
    String val[] = record.split(",");
    IntWritable outputValue;
    Text outputKey;

    try {
      int isCancelled = Integer.parseInt(val[21]);
      if(isCancelled == 1) {
        outputKey = new Text(val[22].toUpperCase().trim().replaceAll("\\s",""));
        outputValue = new IntWritable(1);
        con.write(outputKey, outputValue);
      }
    }
    catch (NumberFormatException ex) {}
  }
}
