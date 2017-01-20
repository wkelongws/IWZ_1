/**
  *****************************************
  *****************************************
  * by Shuo Wang **
  *****************************************
  *****************************************
  */

import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class DataFilter2 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new DataFilter2(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Shuo/note";    // Input
//		String input1 = "Wavetronix_downloadbyShuo/10172015.txt";
//		String input2 = "Wavetronix_downloadbyShuo/10182015.txt";
//		String input3 = "Wavetronix_downloadbyShuo/10192015.txt";
//		String input4 = "Wavetronix_downloadbyShuo/10202015.txt";
//		String input5 = "Wavetronix_downloadbyShuo/10212015.txt";
//		String input6 = "Wavetronix_downloadbyShuo/10222015.txt";
		String output = "Shuo/output";       // Round one output
		//String temp1 = "/scr/shuowang/lab3/exp2/temp1/";     // Round two output
		//String output1 = "/scr/shuowang/lab3/exp2/output1/";   // Round three/final output
		//String output2 = "/scr/shuowang/lab3/exp2/output2/";   // Round three/final output
		
		int reduce_tasks = 1;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();		
		
		Job job_one = new Job(conf, "Shuo_test"); 
		job_one.setJarByClass(DataFilter2.class); 
		job_one.setNumReduceTasks(reduce_tasks);		
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(Text.class);         
		job_one.setOutputValueClass(Text.class);
		job_one.setMapperClass(Map_One.class); 
		job_one.setReducerClass(Reduce_One.class);
		job_one.setInputFormatClass(TextInputFormat.class); 
		job_one.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job_one, new Path(input)); 
//		FileInputFormat.addInputPath(job_one, new Path(input1));
//		FileInputFormat.addInputPath(job_one, new Path(input2));
//		FileInputFormat.addInputPath(job_one, new Path(input3));
//		FileInputFormat.addInputPath(job_one, new Path(input4));
//		FileInputFormat.addInputPath(job_one, new Path(input5));
//		FileInputFormat.addInputPath(job_one, new Path(input6));
		FileOutputFormat.setOutputPath(job_one, new Path(output));
		job_one.waitForCompletion(true); 
			
		return 0;
	
	} // End run
	
	
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {	
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			context.write(new Text("All"),new Text(key.toString()+","+value.toString()));
			
		} // End method "map"
		
	} // End Class Map_One	
	
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			int count=0;
			for (Text val:values)
			{
				count++;
				String[] value = val.toString().split(",");
				if(value.length>1){
				context.write(new Text(Integer.toString(count)),new Text(value[1]));}
			}
			
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	

 	
}
