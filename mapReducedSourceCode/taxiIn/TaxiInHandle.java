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

public class TaxiInHandle {
  public static void main(String [] args) throws Exception {
    Configuration c = new Configuration();

    Path input = new Path("/airport/input/*.csv.bz2");
    Path output = new Path("/airport/output/taxiIn/output");

    Job j=new Job(c,"TaxiInHandle");
    j.setJarByClass(TaxiInHandle.class);
    j.setMapperClass(TaxiInMapper.class);
    j.setReducerClass(TaxiInReducer.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(j, input);
    FileOutputFormat.setOutputPath(j, output);

    System.exit(j.waitForCompletion(true)?0:1);
  }
}
