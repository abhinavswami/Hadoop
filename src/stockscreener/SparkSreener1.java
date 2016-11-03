package stockscreener;

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

public class SparkSreener1 {

	public static void main(String[] args) throws IOException, URISyntaxException {

		if (args.length < 2) {
			System.err.println("Usage: <inputPath> <outputPath>");
			System.exit(1);
		}

		// setup job configuration and context
		SparkConf sparkConf = new SparkConf().setAppName("Stock Screener");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// setup input and output
		JavaRDD<String> inputPath = sc.textFile(args[0], 1);
		// notice, this takes the output path and adds a date and time
		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

		// parse the input line into an array of words
		JavaRDD<String> words = inputPath.flatMap(new FlatMapFunction<String, String>() {
			private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
			private final static int sectorIndex = 5;
			private final static int capIndex = 3;
			private final static int tickerIndex = 0;

			@Override
			public Iterable<String> call(String s) throws Exception {
				String[] tokens = s.split(recordRegex, -1);
				String sectorStr = tokens[sectorIndex].replace("\"", "");
				String tickerStr = tokens[tickerIndex].replace("\"", "");
				String capStr = tokens[capIndex].replace("\"", "");

				String str = sectorStr + "," + tickerStr + "," + capStr;
				return Arrays.asList(str);
				// }
			}
		});

		// define mapToPair to create pairs of <word, 1>
		JavaPairRDD<String, Tuple2<String, Double>> pairs = words
				.mapToPair(new PairFunction<String, String, Tuple2<String, Double>>() {
					private final static double BILLION = 1000000000.00;
					public final static String NO_INFO = "n/a";

					@Override
					public Tuple2<String, Tuple2<String, Double>> call(String word) throws Exception {
						String[] tokens = word.split(",");
						String capStr = tokens[2];
						if (!capStr.equals(NO_INFO) && capStr.endsWith("B")) {
							capStr = capStr.replace("B", "");
							capStr = capStr.replace("$", "");
							Double cap = Double.parseDouble(capStr) * BILLION;
							return new Tuple2<String, Tuple2<String, Double>>(tokens[0],
									new Tuple2<String, Double>(tokens[1], cap));
						}
						return new Tuple2<String, Tuple2<String, Double>>("", new Tuple2<String, Double>("", 0.0));
					}
				});

		/*
		 * define a reduceByKey by providing an *associative* operation that can
		 * reduce any 2 values down to 1 (e.g. two integers sum to one integer).
		 * The operation reduces all values for each key to one value.
		 */
		JavaPairRDD<String, Tuple2<String, Double>> counts = pairs
				.reduceByKey(new Function2<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>() {

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Double> a, Tuple2<String, Double> b)
							throws Exception {

						Double sum = a._2 + b._2;
						return new Tuple2<String, Double>(a._1.toString() + "," + b._1.toString(), sum);
					}
				});

		JavaPairRDD<String, Tuple2<String, Double>> sortedCounts = counts.sortByKey();

		/*-
		* start the job by indicating a save action
		* The following starts the job and tells it to save the output to
		outputPath
		*/
		sortedCounts.saveAsTextFile(outputPath);

		// done
		sc.close();
	}

}
