package example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExampleJob extends Configured implements Tool {

	public static class flightPartitioner extends Partitioner<Flight, Text> {

		@Override
		public int getPartition(Flight key, Text value, int numPartitions) {
			return ((key.getYear() + key.getMonth() + key.getDayofMonth()).hashCode()) % numPartitions;
		}

	}

	public static class flightMapper extends Mapper<LongWritable, Text, Flight, Text> {
		private Flight outputkey = new Flight();
		private Text outputValue = new Text();
		private BufferedReader br;
		private HashMap<String, String> container = new HashMap<>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = StringUtils.split(value.toString(), '\\', ',');
			if (words[0].equals("Year"))
				return;
			if (words.length > 0) {
				for (int i = 0; i < words.length; i++) {
					if (words[i] == null || words[i].equals("NA") || words[i].equals("")) {
						words[i] = "0";
					}
				}
				if (words[17].startsWith("SFO")) {
					outputkey.setYear(words[0].trim());
					outputkey.setMonth(words[1].trim());
					outputkey.setDayofMonth(words[2].trim());
					outputkey.setDepTime(words[4]);
					outputkey.setArrTime(words[6]);
					outputkey.setUniqueCarrier(words[8]);
					outputkey.setFlightNum(words[9]);
					outputkey.setActualElapsedTime(words[11]);
					outputkey.setArrDelay(words[14]);
					outputkey.setDepDelay(words[15]);
					outputkey.setOrigin(words[16]);
					outputkey.setDest(words[17]);
				} else
					return;

			}
			if (container.containsKey(outputkey.getYear() + outputkey.getMonth() + outputkey.getDayofMonth())) {
				outputValue.set(container.get(outputkey.getYear() + outputkey.getMonth() + outputkey.getDayofMonth()));
				context.write(outputkey, outputValue);
			}

		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Flight, Text>.Context context)
				throws IOException, InterruptedException {
			try {
				br = new BufferedReader(new FileReader("sfo_weather.csv"));
				String line = "";
				while ((line = br.readLine()) != null) {
					String[] words = StringUtils.split(line, '\\', ',');
					if (words.length > 0) {
						if (!words[0].startsWith("STATION_"))
							container.put(
									words[1].trim() + new Integer(words[2].trim()).toString()
											+ new Integer(words[3].trim()).toString(),
									words[4] + "," + words[5] + "," + words[6]);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} finally {
				if (br != null)
					br.close();
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(super.getConf());
		job.setJobName("FlightJob");
		Configuration config = job.getConfiguration();
		job.setJarByClass(getClass());

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		out.getFileSystem(config).delete(out, true);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setOutputKeyClass(Flight.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(flightMapper.class);
		job.setReducerClass(Reducer.class);
		job.setCombinerClass(Reducer.class);
		job.setPartitionerClass(flightPartitioner.class);
		job.setGroupingComparatorClass(FlightGroupComparator.class);
		job.setOutputFormatClass(CustomOutputFormat.class);
		job.setOutputKeyClass(Flight.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(2);
		job.addCacheFile(new URI("example2/sfo_weather.csv"));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int result = ToolRunner.run(new Configuration(), new ExampleJob(), args);
		System.exit(result);
	}

}
