package task7;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key,Text value, Context context)throws IOException , InterruptedException{
		
		String line = value.toString().replace("|", ",");                         //convert the value into string 			
		String[] arr= line.split(",");						// split operation carried out 
		String company = arr[0]; 		// putting company name into new string 
		String product = arr[1];        // product name;
		String size = arr[2];
		String join1 = arr[0]+"\t"+arr[1]+"\t"+arr[2] ;    
		
		context.write(new Text(join1), new IntWritable(1));    
		
		
	}

}
