package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class WordCountInMapperCombining.java {

public static class TokenizerMapper 
 extends Mapper<Object, Text, Text, IntWritable> {

 private static final int FLUSH_SIZE = 1000;
 private Map<String, Integer> map;

 public void map(Object key, Text value, Context context) 
  throws IOException, InterruptedException {
  Map<String, Integer> map = getMap();
  StringTokenizer itr = new StringTokenizer(value.toString());
  
  while (itr.hasMoreTokens()) {
   String token = itr.nextToken();
   if(map.containsKey(token)) {
    int total = map.get(token).get() + 1;
    map.put(token, total);
   } else {
    map.put(token, 1);
   }
  }  

  flush(context, false);
 }

 private void flush(Context context, boolean force) 
  throws IOException, InterruptedException {
  Map<String, Integer> map = getMap();
  if(!force) {
   int size = map.size();
   if(size < FLUSH_SIZE)
    return;
  }

  Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
  while(it.hasNext()) {
   Map.Entry<String, Integer> entry = it.next();
   String sKey = entry.getKey();
   int total = entry.getValue().intValue();
   context.write(new Text(sKey), new IntWritable(total));
  }

  map.clear(); //make sure to empty map
 }
 
 protected void cleanup(Context context)
  throws IOException, InterruptedException {
  flush(context, true); //force flush no matter what at the end
  }


 public Map<String, Integer> getMap() {
  if(null == map) 
   map = new HashMap<String, Integer>();
  return map;
 }
}

	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = new Job(conf, "wordcount-java");
	job.setJarByClass(WordCountInMapperCombining.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(WordCountInMapperCombining.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	

	boolean success = job.waitForCompletion(true);
	System.exit(success ? 0 : 1);
}

}