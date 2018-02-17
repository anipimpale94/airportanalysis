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

public class ProbabilityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

    String record = value.toString();
    String val[] = record.split(",");
    DoubleWritable outputValue;
    Text outputKey;

    try {
      int delayTime = Integer.parseInt(val[14]);
      if(delayTime >= 1) {
        outputValue = new DoubleWritable(1);
        outputKey = new Text(val[8].toUpperCase().trim().replaceAll("\\s",""));
        con.write(outputKey, outputValue);
      }
      else {
        outputValue = new DoubleWritable(0);
        outputKey = new Text(val[8].toUpperCase().trim().replaceAll("\\s",""));
        con.write(outputKey, outputValue);
      }
    }
    catch (NumberFormatException ex) {}
  }
}
