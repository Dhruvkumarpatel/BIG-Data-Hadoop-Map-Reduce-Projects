package Kmercounting;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

// Main Class which contains Mapper,Driver and Reducer class
public class Kmercounting {


	// mapper method which take inputs and generate key, value pair
	public static class myMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		// set mers word as key
		Text mers = new Text();
		
		int count = 1;
		
		// set 1 as a value
		IntWritable mersvalue = new IntWritable(count);
		
		// temp variable to store remaining from line
		String tempwindow = "";
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// get the noofmers which is set to configuration in diver class
			Configuration conf = context.getConfiguration();
			
			int noofmers = Integer.parseInt(conf.get("Noof-mers"));
			
			// get the line from input file
			String data = value.toString();
			
			String merssubstring;
			
			// this is for avoid FASTA format line 
			if(data.startsWith(">") == false)
			{

				// add remaining from previous line to infront of next line
				String dnasequence = tempwindow + data;
				
				// generating a noofmers 
				for(int i=0;i<=(dnasequence.length()-noofmers);i++)
				{
				
					if(i == (dnasequence.length()-noofmers))
					{
						merssubstring = dnasequence.substring(i);
						
						// store remaining characters in tempwindow
						tempwindow = dnasequence.substring(i+1);
						
						// set key
						mers.set(merssubstring);
						
						// write key and value in file
						context.write(mers, mersvalue);
						
					}
					else
					{
						// generate noofmers using substring 
						merssubstring = dnasequence.substring(i,i+noofmers);
						
						// set key
						mers.set(merssubstring);
						
						// write key and value in file
						context.write(mers, mersvalue);
					}	
				}
				
				
			}
			
		}

		

	}

	// Reducer class to perform operation to get Correct Output
	public static class myReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
			
		// store key(mers) 
		ArrayList<String> storemers = new ArrayList<String>();

		// store total frequency for particular key(mers)
		ArrayList storefrequency = new ArrayList();

		// this is for sorting an replacement
		ArrayList tempsorting = new ArrayList();

		// variable for final output
		Text finaloutput = new Text();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			// get the noofmers which is set to configuration in diver class
			Configuration conf = context.getConfiguration();
			
			int noofmers = Integer.parseInt(conf.get("Noof-mers"));
			
			int totalfrequence = 0;

			// calculate total frequency for particular key
			for (IntWritable val : values) {
				totalfrequence += val.get();

			}

		
			// This is for to find Top 10
			if (tempsorting.size() == 10) {
				
				// sort 10 size tempsorting arraylist
				Collections.sort(tempsorting);

				// compare new value with lowest in tempsorting
				// if new value higher than lowest just replace new value with lowest 
				// in tempsorting as well as all three arraylist
				if (totalfrequence > (int)tempsorting.get(0)) {
					
					// get the index from storefrequency for lowest value
					int index = storefrequency
							.indexOf(tempsorting.get(0));
					
					// replace new value with lowest one
					storefrequency.set(index, totalfrequence);

					// update key for new value
					storemers.set(index, key.toString());
					
					// replace value in tempsorting
					tempsorting.set(0, totalfrequence);
					
				} else {

				}
			} else {
				
				//add key into storemers
				storemers.add(key.toString());
				
				// add value into storefrequency
				storefrequency.add(totalfrequence);
				
				// add first 10 keys into tempsorting
				tempsorting.add(totalfrequence);
			}
		}

		// cleanup method called at last
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			
			// just read final top10 from tempsorting arraylist 
			for(int i=tempsorting.size()-1;i>=0;i--)
			{
				
				int getindex = storefrequency.indexOf(tempsorting.get(i));
				
				// set mers as key
				finaloutput.set(storemers.get(getindex));
				
				// set null for already readable values
				storefrequency.set(getindex,null);
				
				//wrrite mers as key and frequency as value
				context.write(finaloutput,new IntWritable((int) tempsorting.get(i)));

			}
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

		// set the noofmers if you need for 20 then just change Noof-mers with 20
		conf.set("Noof-mers", "10");

		conf.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));
		
		Job job = new Job(conf,"K-mer Counting");
		job.setJarByClass(Kmercounting.class);
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
