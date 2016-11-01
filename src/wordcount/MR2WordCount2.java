package wordcount;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR2WordCount2 {

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		/**
		 * The reduce method runs once for each key received from the shuffle
		 * and sort phase of the MapReduce framework. The method receives:
		 *
		 * @param Text
		 *            key type
		 * @param IntWritable
		 *            values type
		 * @param Context
		 *            info about the job's config, writers for output
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int wordCount = 0;

			for (IntWritable value : values)
				wordCount += value.get();

			context.write(key, new IntWritable(wordCount));
		}

	}

	/**
	 * To define a map function for your MapReduce job, subclass the Mapper
	 * class and override the map method. The class definition requires four
	 * parameters:
	 *
	 * @param LongWritable
	 *            type of input key
	 * @param Text
	 *            type of input value
	 * @param Text
	 *            type of output key (same type for Reducer input key)
	 * @param IntWritable
	 *            type of output value (same type for Reducer input value)
	 */
	public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		/*-
		 * This function splits each input line into an array of words.
		 * For each word in the array is output as <word, 1>
		 *
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			for (String word : line.split("\\W+")) {
				if (word.length() > 0) {
					context.write(new Text(word), new IntWritable(1));
				}
			}
		}
	}

	/**
	 * The main method defines and starts the word-count job
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out.printf("Usage: Provide <input dir> <output dir>\n");
			System.exit(-1);
		}

		// create job instance
		Job job = Job.getInstance();
		job.setJarByClass(MR2WordCount2.class);
		job.setJobName("Word Count");

		// setup input and output
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// note, this creates a new output path using the time of creation
		String output = args[1] + "_" + Calendar.getInstance().getTimeInMillis();
		FileOutputFormat.setOutputPath(job, new Path(output));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(SumReducer.class);

		/*
		 * Specify the number of reduce tasks
		 *
		 */
		job.setNumReduceTasks(17);

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
