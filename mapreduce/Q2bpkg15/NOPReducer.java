package Q2bpkg15;


import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NOPReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	public static TreeMap<NOP , Text> ToRecordMap = new TreeMap<NOP , Text>(new MyNOPComparator());
	 
	public void map(NullWritable key,Iterable<Text> values, Context context)
	{
		try
		{
			for (Text value : values) {
				 
                String line=value.toString();

                if(line.length()>0){

                	String[] tokens=line.split("\t");

                	int nop=Integer.parseInt(tokens[1]);

                	ToRecordMap.put(new NOP (nop), new Text(value));

                }

			}
			
			
			Iterator<Entry<NOP , Text>> iter = ToRecordMap.entrySet().iterator();
			 
            Entry<NOP , Text> entry = null;



			 while(ToRecordMap.size()>5){
			
			    entry = iter.next();
			
			    iter.remove();            
			
			 }

            for (Text t : ToRecordMap.descendingMap().values()) {

            // Output our ten records to the file system with a null key

            context.write(NullWritable.get(), t);

            }

       
			
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}
