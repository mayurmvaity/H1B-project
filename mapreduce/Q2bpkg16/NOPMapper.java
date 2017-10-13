package Q2bpkg16;







import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NOPMapper extends Mapper<LongWritable,Text,NullWritable, Text> {

	public static TreeMap<NOP, Text> ToRecordMap = new TreeMap<NOP , Text>(new MyNOPComparator());
	
	public void map(LongWritable key, Text value, Context context)
	{
		try
		{
			 String line=value.toString();
			 
             String[] tokens=line.split("\t");

             int nop =Integer.parseInt(tokens[1]);

             ToRecordMap.put(new NOP (nop), new Text(value));
             
             
             Iterator<Entry<NOP , Text>> iter = ToRecordMap.entrySet().iterator();
             
             Entry<NOP , Text> entry = null;
         
                        
         
                         while(ToRecordMap.size()>5){
         
                           entry = iter.next();      
         
                           iter.remove();         
         
                         }
             
             
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	/* cleeanup method */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		 
		// Output our ten records to the reducers with a null key
		 
		 for (Text t:ToRecordMap.values()) {
		 
		     context.write(NullWritable.get(), t);
		 
		  }
	 }   
	
}
