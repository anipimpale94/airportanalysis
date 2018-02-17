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

public class CancelReasonReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  public int max = 0;
  public String reason = "";

  public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
    int eventTotal = 0;

    for(IntWritable value : values) {
      eventTotal = eventTotal + value.get();
    }
    String mainReason = word.toString();

    if(eventTotal > max) {
      max = eventTotal;
      if(mainReason.contains("A")) reason = "A : carrier";
      else if(mainReason.contains("B")) reason = "B : weather";
      else if(mainReason.contains("C")) reason = "C : NAS";
      else if(mainReason.contains("D")) reason = "D : security";
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    context.write(new Text("The most common  reason of flight cancellation"), null);
    context.write(new Text(reason), new IntWritable(max));
  }

}
