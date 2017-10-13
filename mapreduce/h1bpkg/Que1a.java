package h1bpkg;



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


public class Que1a {

	public static class Q1Map extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split("\t");
				long sno = Long.parseLong(str[0]);
				String year = str[7];
				String jobt = str[4];
				
				if(jobt.equals("DATA ENGINEER"))
				{
					context.write(new Text(year), new LongWritable(sno));
				}
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}

	}
	
	public static class Q1Reduce extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
		{
			
			try
			{
				long count = 0l;
				for(LongWritable val: values)
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
	
	private static int k = 0;
	private static long prevcount = 0;
	private static String prevyr = "";
	
	public static class Map12 extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split("\t");
				
				String yr = str[0];
				long count = Long.parseLong(str[1]);
				
				
				if(k > 0)
				{
					String mykey = prevyr+" - "+yr;
					double myval = (double) (count-prevcount)/ (double) prevcount * 100;
					
					context.write(new Text(mykey), new DoubleWritable(myval));
					
				}
				
				prevcount = count;
				prevyr = yr;
				k++;
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
		Job job = Job.getInstance(conf, "Que 1 a");
		job.setJarByClass(Que1a.class);
		
		job.setMapperClass(Q1Map.class);
		job.setReducerClass(Q1Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path mr1op = new Path("firstmrop");
		FileOutputFormat.setOutputPath(job, mr1op);
		
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "2nd mapper");
		job2.setJarByClass(Que1a.class);
		
		job2.setMapperClass(Map12.class);
		job2.setNumReduceTasks(0);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, mr1op);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		System.exit(job2.waitForCompletion(true) ? 0:1);
	}
	
	
}
