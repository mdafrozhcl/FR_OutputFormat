package example;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomOutputFormat extends FileOutputFormat<Flight, Text> {

	public static class CustomRecordWriter extends RecordWriter<Flight, Text> {
		private DataOutputStream out;

		public CustomRecordWriter(DataOutputStream fileOutput) {
			super();
			this.out = fileOutput;
		}

		@Override
		public void write(Flight key, Text value) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			builder.append(key.toString()).append(",").append(value.toString()).append("\n");
			out.writeUTF(builder.toString());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			out.close();
		}
	}

	@Override
	public RecordWriter<Flight, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		int partition = context.getTaskAttemptID().getTaskID().getId();
		Path outputDir = FileOutputFormat.getOutputPath(context);
		FileSystem fs = outputDir.getFileSystem(context.getConfiguration());
		Path file = new Path(outputDir + Path.SEPARATOR + context.getJobName().toString() + "_" + partition);
		FSDataOutputStream fileOut = fs.create(file, context);
		return new CustomRecordWriter(fileOut);
	}
}