package Table0;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Table0 extends Configured implements Tool {
	
	 
	public static class mapper1 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text key = new Text();
	
		
		
		public void map(LongWritable k,Text value,Context context) throws IOException, InterruptedException
		{
				String line = value.toString();
			
				String words[] = line.split("<SEP>");
				
				key.set(words[words.length-2]);
				
				value.set(words[words.length-1]+"mapper1");
				
				context.write(key, value);
				
		//		System.out.println(key+":"+value);
	
		}
		
	}
	
	
	public static class mapper2 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text key = new Text();
		
		public void map(LongWritable k,Text value,Context context) throws IOException, InterruptedException
		{
				String line = value.toString();
			
				String words[] = line.split("<SEP>");
				
				key.set(words[3]);
				
				value.set(words[0]+"mapper2");
				
				context.write(key, value);	
		}
		
	} 

	public static class mapper3 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text key = new Text();

		public void map(LongWritable k,Text value,Context context) throws IOException, InterruptedException
		{
				String line = value.toString();
			
				String words[] = line.split("<SEP>");
				
				key.set(words[words.length-2]);
				
				value.set(words[words.length-1]+"mapper3");
				
				context.write(key, value);
				
			//	System.out.println(key+":"+value);
	
		}
		
	}
	
	
	public static class reducer extends Reducer<Text,Text,Text,Text>
	{
		
		Text newkey = new Text();
		
		Text value = new Text();
		
		String temp = "" ;
		
		String finalvalue = "";
		
		ArrayList<String> store = new ArrayList<String>(3);
		
		
		
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			store.add(0,"");
			store.add(1,"");
			store.add(2,"");
			
			
			int i = 0;
			
			
			for(Text value:values)
			{
				
				if(value.toString().contains("mapper1"))
				{
					
					
					if(i==0)
					{
					
						String temp1 = value.toString().replaceAll("mapper1"," ");
						temp = "1$" + temp1.trim();
						
					}
					else
					{
						String temp1 = value.toString().replaceAll("mapper1"," ");
						temp = temp + "<I>" + temp1.trim();
				
					}
					
					i++;
					
						
				}
				else if(value.toString().contains("mapper2"))
				{
					if(i == 0)
					{
					
						String temp2 = value.toString().replaceAll("mapper2"," ");
						temp = "2$" + temp2.trim();
						
					}
					else
					{
						String temp2 = value.toString().replaceAll("mapper2"," ");
						temp = temp + "<I>" + temp2.trim();
				
					}
					
					i++;
																
				}
				else if (value.toString().contains("mapper3"))
				{
					if(i==0)
					{
					
						String temp3 = value.toString().replaceAll("mapper3"," ");
						temp = "3$" + temp3.trim();
						
					}
					else
					{
						String temp3 = value.toString().replaceAll("mapper3"," ");
						temp = temp + "<I>" + temp3.trim();
				
					}
					
					i++;
					
				} 
				else
				{
					
					String none = value.toString();
					
					if(none.startsWith("1$"))
					{
						
						String store1 = none.replace("1$"," ");
						
				//		System.out.println("store1 :"+store1);
						
						store.set(0,store1.trim());
						
				//		System.out.println("store1 :"+store1);
						
					}
					else if(none.startsWith("2$"))
					{
						String store2 = none.replace("2$"," ");
						store.set(1,store2.trim());
					}
					else if(none.startsWith("3$"))
					{
						String store3 = none.replace("3$"," ");
						store.set(2,store3.trim());
					}
					
					
					
					temp = store.get(1)+"<SEP>"+key.toString()+"<SEP>" + store.get(2)+"<SEP>"+store.get(0);
					
					//	System.out.println("finalvalue :"+key.toString());
					
					
				} 

			}
				
			value.set(temp);
			
	//		String k="";
			
			context.write(key,value);
			
			//System.out.println("all done");
			
			} 
	
	}
	
	public static void main(String[] args) throws Exception {
	   
		int exit = ToolRunner.run(new Configuration(),new Table0(), args);
		
		System.exit(exit);
		
	  } 

	 public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
	
		 	Configuration conf = new Configuration();
		    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		  
		    Job job = Job.getInstance(conf, "Task0");
		    job.setJarByClass(Table0.class);
		
		    
		    job.setNumReduceTasks(1);
		    
		    job.setCombinerClass(reducer.class);
		    
		    job.setReducerClass(reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    MultipleInputs.addInputPath(job,new Path(args[0]+"/unique_tracks.txt"),TextInputFormat.class,mapper1.class);
		    MultipleInputs.addInputPath(job,new Path(args[0]+"/unique_artists.txt"),TextInputFormat.class,mapper2.class);
		    MultipleInputs.addInputPath(job,new Path(args[0]+"/input0/artist_location.txt"),TextInputFormat.class,mapper3.class);
		   
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		    
		   return (job.waitForCompletion(true) ? 0 : 1);
	
	}


}
