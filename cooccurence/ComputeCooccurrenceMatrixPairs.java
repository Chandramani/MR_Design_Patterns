
package cooccurence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import util.TextPair;

/**
   Implementation of the "pairs" algorithm for computing co-occurrence
 matrices from a large text collection. 

  Arguments 
  [input-path]
  [output-path]
  [window]
 [num-reducers]
 
 */
public class ComputeCooccurrenceMatrixPairs extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ComputeCooccurrenceMatrixPairs.class);

	private static class MyMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

		private final TextPair pair = new TextPair();
		private final IntWritable one = new IntWritable(1);
		private int window = 2;

		@Override
		public void setup(Context context) {
			window = context.getConfiguration().getInt("window", 2);
		}

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException,
				InterruptedException {
			String text = line.toString();

			String[] terms = text.split("\\s+");

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				// skip empty tokens
				if (term.length() == 0)
					continue;

				for (int j = i - window; j < i + window + 1; j++) {
					if (j == i || j < 0)
						continue;

					if (j >= terms.length)
						break;

					// skip empty tokens
					if (terms[j].length() == 0)
						continue;

					pair.set(term, terms[j]);
					context.write(pair, one);
				}
			}
		}
	}

	private static class MyReducer extends
			Reducer<TextPair, IntWritable, TextPair, IntWritable> {

		private final static IntWritable SumValue = new IntWritable();

		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}

			SumValue.set(sum);
			context.write(key, SumValue);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public ComputeCooccurrenceMatrixPairs() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [input-path] [output-path] [window] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int window = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: ComputeCooccurrenceMatrixPairs");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - window: " + window);
		sLogger.info(" - number of reducers: " + reduceTasks);

		Job job = new Job(getConf(), "CooccurrenceMatrixPairs");

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(getConf()).delete(outputDir, true);

		job.getConfiguration().setInt("window", window);

		job.setJarByClass(ComputeCooccurrenceMatrixPairs.class);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ComputeCooccurrenceMatrixPairs(), args);
		System.exit(res);
	}
}
