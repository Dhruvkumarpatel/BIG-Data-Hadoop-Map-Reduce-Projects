package Table2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;




public class Table2 {
	
	public static class mapper1 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text key = new Text();
		
		public void map(LongWritable k,Text value,Context context) throws IOException, InterruptedException
		{
			
			String line = value.toString();
			
			String words[] = line.split("<SEP>");
			
			String location = words[0];
			
			String updatedvalue = "";
			
			HashMap<Text,ArrayList<String>> map = new HashMap<Text,ArrayList<String>>();
			
			for(int i=1;i<words.length;i++)
			{
				
				updatedvalue += words[i];
				
			}
			
			System.out.println(key+":"+value);
			
			key.set(location);
			
			value.set(updatedvalue);
			
			
		}
		
	}
	
	public static class reducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			
			
			
			
			
		}
		
	}
	
	static {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.addAppender((Appender) new ConsoleAppender(
                new PatternLayout("%-6r [%p] %c - %m%n")));

    } 
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
	    conf.set(TextOutputFormat.SEPERATOR,"");
	    Job job = Job.getInstance(conf, "word count");
	    
	    job.setJarByClass(Table2.class);
	    job.setMapperClass(mapper1.class);
	    job.setCombinerClass(reducer.class);
	    job.setReducerClass(reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]+"/Table1.txt"));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
