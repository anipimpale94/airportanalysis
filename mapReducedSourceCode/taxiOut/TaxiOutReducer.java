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

public class TaxiOutReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  HashMap<String, Double> map = new HashMap<String, Double>();

  public void reduce(Text word, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {
    double eventTotal = 0;
    double lateEventTotal = 0;

    for(DoubleWritable value : values) {
      eventTotal = eventTotal + 1;
      lateEventTotal = lateEventTotal + value.get();
    }
    String mainWord = word.toString();
    double average = lateEventTotal/eventTotal;

    map.put(mainWord, average);

  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {

    int i = 0, j = 3;
    String[] max = new String[3];
    Comparator<String> comparator = new ValueComparator<String, Double>(map);
    TreeMap<String, Double> result = new TreeMap<String, Double>(comparator);
    result.putAll(map);
    int totalElements = result.size();

    context.write(new Text("Lowest Average of Taxi Out Time"), null);
    for (Map.Entry<String, Double> entry : result.entrySet()) {
      if(i < 3)
        context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
      if((totalElements - i) <= 3) {
        j--;
        max[j] = entry.getKey();
      }
      i++;
    }
    context.write(new Text("Highest Average of Taxi Out Time"), null);
    for(int k = 0; k < 3; k++){
      context.write(new Text(max[k]), new DoubleWritable(map.get(max[k])));
    }
  }
}


class ValueComparator<K, V extends Comparable<V>> implements Comparator<K>{

	HashMap<K, V> map = new HashMap<K, V>();

	public ValueComparator(HashMap<K, V> map){
		this.map.putAll(map);
	}

	@Override
	public int compare(K s1, K s2) {
		return map.get(s1).compareTo(map.get(s2));
	}
}
