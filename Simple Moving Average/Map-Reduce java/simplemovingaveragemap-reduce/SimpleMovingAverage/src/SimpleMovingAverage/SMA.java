package SimpleMovingAverage;

// All the imports for Hadoop Map-Reduce program
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

// Main class which contains Mapper, Reducer and Driver Class 
public class SMA {

	// This is used for format Date
	private static SimpleDateFormat simpledateformat = new SimpleDateFormat(
			"yyyy-MM-dd");

	// Mapper class which contains Inputkey,InputValue,OutputKey,OutputValue
	public static class Mymapper extends
			Mapper<LongWritable, Text, Text, Dateclosingprice> {
		// This is for set company symbol as a key
		Text Companysymbol = new Text();

		// this is the class which implements WritableComparable interface
		private static Dateclosingprice dateclosingprice;

		// map function generate Key and value pair
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Define class Object
			dateclosingprice = new Dateclosingprice();

			// Read the value from the file
			String line = value.toString();

			String data[] = line.split(",");

			// set company symbol as a key
			Companysymbol.set(data[0]);

			Date d = null;

			try {

				// convert String into specific Date format
				d = simpledateformat.parse(data[data.length - 2]);

				// set Date and Closing price into Dateclosingprice class in
				// Long and Double format Respectively
				dateclosingprice.setNumericdate(d.getTime());

				dateclosingprice.setClosingprice(Double
						.parseDouble(data[data.length - 1]));

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// Just Print MApper output into Console
			System.out.println(Companysymbol + ":" + simpledateformat.format(d)
					+ ":" + Double.parseDouble(data[data.length - 1]));

			// write key and value pair
			context.write(Companysymbol, dateclosingprice);
		}
	}

	// Reducer Class to take the Data from intermediate in sorted order and
	// calculate Simple moving average
	public static class Myreducer extends
			Reducer<Text, Dateclosingprice, Text, Text> {

		// set the output value
		Text value = new Text();

		// Manually enter window size
		// if you want to enter 4 then change manually here.
		int windowsize = 3;

		// reduce function calculate simple moving average according to company
		// symbol and date
		public void reduce(Text key, Iterable<Dateclosingprice> values,
				Context context) throws IOException, InterruptedException {

			String temp = "";

			// store the Dateclosingprice class object in List
			List<Dateclosingprice> list = new ArrayList<Dateclosingprice>();

			for (Dateclosingprice object : values) {

				list.add(new Dateclosingprice(object.getNumericdate(), object
						.getClosingprice()));
			}

			// call this methodd to inable compareTo in Dateclosingprice
			Collections.sort(list);

			// Retrive sorted values from list
			for (int i = 0; i < list.size(); i++) {

				Dateclosingprice object = list.get(i);

				Date date = new Date(object.getNumericdate());

				// call calculatesma class and return simple moving average
				Double SMA = calculatesma.calculation(windowsize,
						object.getClosingprice());

				// set output value as output format
				temp = "[" + simpledateformat.format(date) + "]" + "[,]" + "["
						+ SMA + "]";

				// set temp as a value
				value.set(temp);

				// make key format
				String keyformat = "[" + key + "]";

				// set company symbol as a key
				key.set(keyformat);

				// just print the reduce output in console
				System.out.println(key.toString() + ":" + temp);

				// write output into output file
				context.write(key, value);

				// remove others from key and use for the next samples
				key.set(key.toString().substring(1, keyformat.length() - 1));
			}

		}
	}

	// this is just a Logger which is work as try-catch in hadoop for trace the
	// errors.
	static {
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.INFO);
		rootLogger.addAppender((Appender) new ConsoleAppender(
				new PatternLayout("%-6r [%p] %c - %m%n")));

	}

	// Driver class Main Method
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		conf.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));

		Job job = new Job(conf, "SimpleMovingAverage");
		// set driver class
		job.setJarByClass(SMA.class);

		// set Mapper class
		job.setMapperClass(Mymapper.class);
		// set Reducer class
		job.setReducerClass(Myreducer.class);

		// final output key type
		job.setOutputKeyClass(Text.class);
		// Output value type from mapper class
		job.setMapOutputValueClass(Dateclosingprice.class);
		// final output value type
		job.setOutputValueClass(Text.class);
		// output key type from mapper class
		job.setMapOutputKeyClass(Text.class);

		// set the arguments for input and output directory
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
