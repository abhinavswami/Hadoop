package wordcount;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkWordCount {

	public static void main(String[] args) throws IOException, URISyntaxException {

		if (args.length < 2) {
			System.err.println("Usage: <inputPath> <outputPath>");
			System.exit(1);
		}

		// setup job configuration and context
		SparkConf sparkConf = new SparkConf().setAppName("WordCount");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// setup input and output
		JavaRDD<String> inputPath = sc.textFile(args[0], 1);
		// notice, this takes the output path and adds a date and time
		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

		// parse the input line into an array of words
		JavaRDD<String> words = inputPath.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) throws Exception {
				return Arrays.asList(s.split("\\W+"));
			}
		});

		// define mapToPair to create pairs of <word, 1>
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		/*
		 * define a reduceByKey by providing an *associative* operation that can
		 * reduce any 2 values down to 1 (e.g. two integers sum to one integer).
		 * The operation reduces all values for each key to one value.
		 */
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});

		JavaPairRDD<String, Integer> sortedCounts = counts.sortByKey();

		/*-
		 * start the job by indicating a save action
		 * The following starts the job and tells it to save the output to outputPath
		 */
		sortedCounts.saveAsTextFile(outputPath);

		// done
		sc.close();
	}
}
