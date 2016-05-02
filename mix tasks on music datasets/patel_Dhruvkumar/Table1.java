package Table1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.*;

public class Table1{
	
	public static class mapper1 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text key = new Text();
		
		public void map(LongWritable k,Text value,Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			
			String words[] = line.split("<SEP>");
			
			String artistname = words[1];
			
			System.out.println("artistname:"+artistname);
			
			value.set(artistname);
			
			String locationstring = words[words.length-2];
			
		if(!locationstring.equals(""))
		{
			
			if(locationstring.contains("<I>"))
			{
				String location[] = locationstring.split("<I>");
				
				for(int i=0;i<location.length;i++)
				{
					
					
					key.set(location[i]);
					
				//	System.out.println(key+":"+value);
					
					context.write(key, value);
				}
			}
			else
			{
					key.set(locationstring);
					
					context.write(key, value);
				//	System.out.println(key+":"+value);
			}
			
			
		}	
			
			
		}
	}
	
	public static class reducer extends Reducer<Text,Text,Text,Text>
	{
		Text value = new Text();
		
		String tempval = "";
		
		
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int i=0;
			
			for(Text val:values)
			{
				if(i==0)
				{
					tempval = val.toString();
				}
				else
				{
					tempval += "<SEP>" + val.toString();
				}
				
				i++;
			}
			
			
			
			value.set(tempval);
			
				context.write(new Text(key.toString()), value);
			
			
			
			
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
	    conf.set(TextOutputFormat.SEPERATOR,"<SEP>");
	    Job job = Job.getInstance(conf, "word count");
	    
	    job.setJarByClass(Table1.class);
	    job.setMapperClass(mapper1.class);
	    job.setCombinerClass(reducer.class);
	    job.setReducerClass(reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]+"/Table0.txt"));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	  }
	
	
	
	
	
	
}