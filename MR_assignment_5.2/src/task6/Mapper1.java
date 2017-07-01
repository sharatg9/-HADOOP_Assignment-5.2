package task6;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key,Text value, Context context)throws IOException , InterruptedException{
		
		String line = value.toString().replace("|", ",");                         //convert the value into string 		
		String[] arr= line.split(",");						                      // split operation carried out      
		
		int flag=0;
		if(arr[0].equalsIgnoreCase("Onida")){                     
			flag=1;                                                             
		}
		
	    if(flag==1){
	    	
	    	String state = arr[3];                                           // putting state name into new string
			context.write(new Text(state), new IntWritable(1));             //  ex : uttar pradesh ,1 
		}
	}

}
