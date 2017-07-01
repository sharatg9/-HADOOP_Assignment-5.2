package task6;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override 
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		int sum = 0;
		for (IntWritable val : values){                 // for loop to count the Int values which is in the array 
			sum = sum+val.get();
		}
		
		context.write(key, new IntWritable(sum));       // state wise count 
		
		
	}
		
}
