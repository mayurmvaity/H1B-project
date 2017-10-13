package Q2bpkg12;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2bDriver {

	public static class Q2bMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String str[] = value.toString().split("\t");
				
				String casestat = str[1];
				String loc = str[8];
				String yr = str[7];
				
				if(yr.equals("2012") && casestat.equals("CERTIFIED"))
				{
					context.write(new Text(loc), value);
				}
				
				
				
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}

	public static class Q2bRed1 extends Reducer<Text,Text,Text,LongWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)
		{
			try
			{
				long count = 0;
				for(Text val: values)
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
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "1st mapper");
		
		job.setJarByClass(Q2bDriver.class);
		
		job.setMapperClass(Q2bMapper.class);
		job.setReducerClass(Q2bRed1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path op1 = new Path("q2bop1temp");
		FileOutputFormat.setOutputPath(job, op1);
		
		job.waitForCompletion(true);
		
		
		Job job2  = Job.getInstance(conf, "2nd process");
		
		job2.setJarByClass(Q2bDriver.class);
		
		job2.setMapperClass(NOPMapper.class);
		job2.setReducerClass(NOPReducer.class);
		
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, op1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
