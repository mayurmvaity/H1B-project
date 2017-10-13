package h1bpkg;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Que2a {

	public static class Q2aMap extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split("\t");
				
				String year = str[7];
				String jobt = str[4];
				String wsite = str[8];
				long sno = Long.parseLong(str[0]);
				
				if(jobt.equals("DATA ENGINEER"))
				{
					String mykey = year+"\t"+wsite;
					
					context.write(new Text(mykey), new LongWritable(sno));
				}
				
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static class Q2aReducer extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
		{
			try
			{
				long count = 0l;
				for(LongWritable val:values)
				{
					count++;
				}
				
				context.write(key, new LongWritable(count));
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
		
	}
	
	public static class Map2 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split("\t");
				String yr = str[0];
				String loc = str[1];
				Long count = Long.parseLong(str[2]);
				
				String myval = loc+"\t"+count;
				context.write(new Text(yr), new Text(myval));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static class Red2 extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)
		{
			try
			{
				long maxCount = 0;
				String maxloc = "";
				for(Text val: values)
				{
					String str[] = val.toString().split("\t");
					String loc = str[0];
					long count = Long.parseLong(str[1]);
					
					if(maxCount < count)
					{
						maxCount = count;
						maxloc = loc;
					}
					
				}
				
				context.write(key, new Text(maxloc+"\t"+maxCount));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q 2 a");
		job.setJarByClass(Que2a.class);
		
		job.setMapperClass(Q2aMap.class);
		job.setReducerClass(Q2aReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path firstop = new Path("FirstOutput");
		FileOutputFormat.setOutputPath(job, firstop);
		
		job.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(conf, "2nd job");
		job2.setJarByClass(Que2a.class);
		
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Red2.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, firstop);
		Path secondop = new Path(args[1]);
		FileOutputFormat.setOutputPath(job2, secondop);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
