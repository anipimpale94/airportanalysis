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

public class TaxiOutMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

    String record = value.toString();
    String val[] = record.split(",");
    DoubleWritable outputValue;
    Text outputKey;

    try {
      int taxiTime = Integer.parseInt(val[20]);
      if(taxiTime > 0) {
        outputValue = new DoubleWritable(taxiTime);
        outputKey = new Text(val[16].toUpperCase().trim().replaceAll("\\s",""));
        con.write(outputKey, outputValue);
      }
    }
    catch (NumberFormatException ex) {}
  }
}
