// using the created mypackage.inverted_index package
package mypackage.stopwords;

// using libraries and functions for the format, mapper, reducer, ...
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

//using IOException to catch the exceptions
import java.io.IOException;
import java.util.Arrays;

// We count and catch the words that we count more than 4000 times
public class stopwords_10_reducers_with_combiners_with_compressor extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new stopwords_10_reducers_with_combiners_with_compressor(), args);
		System.exit(res);
	}
	
	@Override
	//job conf
	public int run(String[] args) throws Exception {
		// Initializing the job and the sys out
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "stopwords_10_reducers_with_combiners_with_compressor");
		// Initializing the key/values, mapper/reducer and the input/ouput format
		job.setJarByClass(stopwords_10_reducers_with_combiners_with_compressor.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// THIS IS WHERE WE ADD THE COMBINER
		job.setCombinerClass(Reduce.class);		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// setting up the configuration
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
		
		// THIS IS WHERE WE COMPRESS THE OUTPUT
		job.getConfiguration().set("mapred.compress.map.output", "true");
		job.getConfiguration().set("mapred.output.compression.type", "BLOCK");
		job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		// THIS IS WHERE WE CHOOSE THE NUMBER OF REDUCERS
		job.setNumReduceTasks(10);
		
		// take the two given arguments and give them as the input and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);}

		job.waitForCompletion(true);
		return 0;
	}
	
	// Mapping class
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			for (String token : value.toString().split("\\s+")) {
				// we consider only lowercase tokens
				word.set(token.toLowerCase());
				context.write(word, ONE);
			}
		}
	}

	// Reducing class
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			// adding the counting
			for (IntWritable val : values) {
				sum += val.get();}
			// We select here the words to keep or not
			if (sum > 4000) {
				context.write(key, new IntWritable(sum));}
		}
	}
}