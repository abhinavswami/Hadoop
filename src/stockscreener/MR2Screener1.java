package stockscreener;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce application to list the tickers for each sector. Uses counters to:
 * 1. count number of stocks per sector. 2. sum the total capitalization for a
 * sector
 *
 * Uses NASDAQ company list file
 *
 * @author AbhinavSwami
 *
 */

public class MR2Screener1 {

	/*-
	 * Mapper for parsing sector info out of file, writing out the sector, ticker and capital
	 *
	 * Example record in the NASDAQ company list files:
	 *
	 * "GOOG","Google Inc.","523.4","$357.66B","2004","Technology","Computer Software","http://www.nasdaq.com/symbol/goog",
	*/
	public static class StockSectorMapper extends Mapper<Object, Text, Text, Text> {

		// Create holders for output so we don't recreate on every map

		private final static Text TICKER_CAP = new Text();
		private final static Text KEY = new Text();

		/*-
		 * example of a data record in the NASDAQ company list files
		 * "GOOG","Google Inc.","523.4","$357.66B","2004","Technology","Computer Software","http://www.nasdaq.com/symbol/goog",
		*/
		private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
		private final static int sectorIndex = 5;
		private final static int capIndex = 3;
		private final static int tickerIndex = 0;
		private final static double BILLION = 1000000000.00;
		public final static String NO_INFO = "n/a";

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(recordRegex, -1);
			String sectorStr = tokens[sectorIndex].replace("\"", "");
			String tickerStr = tokens[tickerIndex].replace("\"", "");
			String capStr = tokens[capIndex].replace("\"", "");

			// skip the header
			if (sectorStr.equals("Sector"))
				return;

			if (tokens.length == 9) {

				// Look for B at the end indicating Billion
				if (!sectorStr.equals(NO_INFO) && capStr.endsWith("B")) {
					capStr = capStr.replace("B", "");
					capStr = capStr.replace("$", "");

					KEY.set(sectorStr);
					TICKER_CAP.set(tickerStr + "," + Double.parseDouble(capStr) * BILLION);

					context.write(KEY, TICKER_CAP);
				}
			}
		}
	}

	/*
	 * Reducer for parsing the mapper output. Receives sector, tickers and
	 * capital as input.
	 * 
	 * Writes sector name, total tickers in the sector and total capitalization
	 * of the sector as the output.
	 */

	public static class StockSectorReducer extends Reducer<Text, Text, Text, Text> {

		private final static Text SECTOR_INFO = new Text();
		private final static Text KEY = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			float dcap = 0;
			long lcap = 0;
			long totalTickers = 0;
			long totalCapital = 0;
			StringBuilder tickers = new StringBuilder();

			for (Text value : values) {
				String tokens[] = value.toString().split(",");
				tickers.append(tokens[0] + ", ");
				dcap = Float.valueOf(tokens[1]);
				lcap = (long) dcap;
				context.getCounter("SectorCount", key.toString() + "total capital").increment(lcap);
				context.getCounter("SectorCount", key.toString() + "total tickers").increment(1);
			}

			totalTickers = context.getCounter("SectorCount", key.toString() + "total tickers").getValue();
			totalCapital = context.getCounter("SectorCount", key.toString() + "total capital").getValue();

			String info = "\n" + "Tickers = " + tickers.toString() + "\n" + "Total tickers for " + key.toString()
					+ " = " + totalTickers + "\n" + "Total capitalization for " + key.toString() + "= $" + totalCapital
					+ "\n";

			KEY.set("Sector = " + key);
			SECTOR_INFO.set(info);
			context.write(KEY, SECTOR_INFO);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if (args.length != 2) {
			System.out.printf("Usage: MR2Screener1 <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = Job.getInstance();

		job.setJarByClass(MR2Screener1.class);
		job.setJobName("Stock screener");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		String output = args[1] + "_" + Calendar.getInstance().getTimeInMillis();
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(StockSectorMapper.class);
		job.setReducerClass(StockSectorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		// set output key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
