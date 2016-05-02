package MutualFriends;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

// Main Class which contains Mapper, Driver and Reducer class
public class commonfriends {

	// mapper method which take inputs and generate key, value pair
	public static class myMapper extends Mapper<LongWritable, Text, Text, Text> {

		// variable contains userid pair as key
		Text useridpair = new Text();

		// Friendslist as value
		Text friendslist = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();

			String split[] = input.split(",");

			String friends[] = split[1].split(" ");

			for(int i = 0; i < friends.length; i++) {
				
				String makekey = null;

				// compare userid pairs so both a,b and b,a (For example, 101,102 and
				// 102,101 work as a same key)

				if (split[0].compareTo(friends[i]) < 0) {
					makekey = split[0] + "," + friends[i];
				}else {
					makekey = friends[i] + "," + split[0];
				}
				
				// set key and value pair
				useridpair.set(makekey);
				friendslist.set(split[1]);

				context.write(useridpair, friendslist);
			}

		}

	}

	// Reducer class to perform operation to get Correct Output
	public static class myReducer extends Reducer<Text, Text, Text, Text> {

		Text outputvalue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String temp = "";

			// loop through the each and every value
			for (Text val : values) {

				temp += ":" + val.toString().trim();

			}

			String temp1[] = temp.split(":");
			
			// set common friends in output which are returns from the common methods
			outputvalue.set(Findcommonfriends.common(temp1[1], temp1[2]));

			// write output in the file
			context.write(key, outputvalue);

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

		conf.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));

		Job job = new Job(conf,"Find Common Mutual Friends");
		job.setJarByClass(commonfriends.class);
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}