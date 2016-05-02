package MatrixMultiplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

// Main Class which contains Mapper, Driver and Reducer class
public class MatrixMultiplication {

	// Mapper function which takes input from file and generate key,value pair
	// as an output
	public static class myMapper extends Mapper<LongWritable, Text, Text, Text> {

		Text keyformatrixrow = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// here i take value which i set in driver class
			Configuration config = context.getConfiguration();

			// get the argument which indicates column size of 2nd Matrix
			int k = Integer.parseInt(config.get("k"));

			// get the argument which indicates row size of 1st Matrix
			int i = Integer.parseInt(config.get("i"));

			String matrixdata = value.toString();

			// if line starts with A then
			if (matrixdata.startsWith("A")) {

				String firstmatrixdata[] = matrixdata.split(",");

				for (int n = 0; n < k; n++) {

					// key would be number upto 2nd matrix column size and
					// column of second matrix
					String keydata = firstmatrixdata[1] + "," + n;

					keyformatrixrow.set(keydata);

					// Remaining is value it contains
					// Matrixname,columnindex,value
					String keyvalue = firstmatrixdata[0] + ","
							+ firstmatrixdata[2] + "," + firstmatrixdata[3];

					value.set(keyvalue);

					// System.out.println(keyformatrixrow + ":" + value);

					// set the key,value pair for Reducer
					context.write(keyformatrixrow, value);
				}

			}
			// if line startswith B then
			else {

				String firstmatrixdata[] = matrixdata.split(",");

				// set the key as value from loop upto first matrix row size and
				// column of second Matrix
				for (int n = 0; n < i; n++) {

					String keydata = n + "," + firstmatrixdata[2];

					keyformatrixrow.set(keydata);

					// value is Matrixname,secondmatrix row index,value
					String keyvalue = firstmatrixdata[0] + ","
							+ firstmatrixdata[1] + "," + firstmatrixdata[3];

					value.set(keyvalue);

					// System.out.println(keyformatrixrow + ":" + value);

					// set the key,value pair for Reducer
					context.write(keyformatrixrow, value);
				}

			}

		}

	}

	// Reducer class to perform operation to get Correct Output
	public static class myReducer extends
			Reducer<Text, Text, Text, DoubleWritable> {

		// set the Result as Double in value for final result
		DoubleWritable value = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			// get the size of a two matrices

			int i = Integer.parseInt(config.get("i"));
			int j = Integer.parseInt(config.get("j"));
			int k = Integer.parseInt(config.get("k"));

			// Store values in two matrices according to A and B in Respectively
			// matrix1 and matrix2
			// In my case i do not required to sort all values by j
			double matrix1[][] = new double[i][j];
			double matrix2[][] = new double[j][k];

			// split the key for i and k
			String getifromkey[] = key.toString().split(",");

			// this counts how many columns are there with A
			int count = 0;

			for (Text val : values) {

				// split value with ,
				String temp[] = val.toString().split(",");

				// if starts with A then store in matrix1 using i and j indexes
				if (val.toString().startsWith("A")) {

					matrix1[Integer.parseInt(getifromkey[0])][Integer
							.parseInt(temp[1])] = Double.parseDouble(temp[2]);
					// increment count with every entry of A
					count++;
				}
				// if starts with B then store in matrix2 using j and k indexes
				else {
					matrix2[Integer.parseInt(temp[1])][Integer
							.parseInt(getifromkey[1])] = Double
							.parseDouble(temp[2]);

				}

			}

			// result is for store resulting value
			double result = 0;

			// calculate Matrix multiplication and store result in final value
			for (int l = 0; l < count; l++) {

				// Avoid 0 value from matrix1 and matrix2
				// satisfy your condition here it contains only non-zero
				// elements of each matrix for
				// efficiency and the matrix value is real-value
				if ((matrix1[Integer.parseInt(getifromkey[0])][l] != 0)
						&& (matrix2[l][Integer.parseInt(getifromkey[1])] != 0)) {

					result = result
							+ matrix1[Integer.parseInt(getifromkey[0])][l]
							* matrix2[l][Integer.parseInt(getifromkey[1])];
				}
			}

			// set the output as value
			value.set(result);

			// write output in output file
			context.write(key, value);
		}

	}

	// this is for error tracking in hadoop map-reduce like try-catch in java
	static {
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.INFO);
		rootLogger.addAppender((Appender) new ConsoleAppender(
				new PatternLayout("%-6r [%p] %c - %m%n")));

	}

	// Driver class to execute hadoop Map-Reduce
	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		// Enter the matrix row and column name and size
		// first matrix i*j and second j*k
		// explicitly change size of matrices according to input file
		conf.set("i", "10");
		conf.set("j", "10");
		conf.set("k", "5");

		conf.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));

		Job job = new Job(conf, "Matrix Multiplication");
		job.setJarByClass(MatrixMultiplication.class);
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
