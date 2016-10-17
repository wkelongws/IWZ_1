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



public class TransSuiteConvert extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new TransSuiteConvert(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Shuo/makeup.txt";    // Input
		String temp = "Shuo/output";       // Round one output
		//String temp1 = "/scr/shuowang/lab3/exp2/temp1/";     // Round two output
		//String output1 = "/scr/shuowang/lab3/exp2/output1/";   // Round three/final output
		//String output2 = "/scr/shuowang/lab3/exp2/output2/";   // Round three/final output
		
		int reduce_tasks = 12;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1: round 1 gets all the neighbors of A and the number of triplets with A in the middle, output to 'temp'
		
		// Create the job
		Job job_one = new Job(conf, "Exp2 Program Round One"); 	
		
		// Attach the job to this Driver
		job_one.setJarByClass(TransSuiteConvert.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);		
		
//		job_one.setMapOutputKeyClass(Text.class); 
//		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);         
		job_one.setOutputValueClass(Text.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
		// The class that provides the reduce method
//		job_one.setReducerClass(Reduce_One.class);
		
		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
	
		return 0;
	
	} // End run
	
	// The round one: round 1 gets all the neighbors of A and the number of triplets with A in the middle

	public static class Map_One extends Mapper<LongWritable, Text, NullWritable, Text>  {		
	
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line (edge) from the input
			String line = value.toString();
			
			// Split the edge into two nodes 
			String[] nodes = line.split(",");
			
			if (!nodes[0].equals("IntId"))
			{
				int weightedspeedsum = 0;			
				int countsum = 0;			
				int occupancysum = 0;
				double avgoccupancy = 0.0;
				double avgspeed = 0.0;
				int smallcountsum = 0;
				int middlecountsum = 0;
				int largecountsum = 0;
				
				String timestamp = nodes[4];
				
				
				String date = timestamp.split(" ")[0];
				
				String yy = date.split("/")[2];
				String m = date.split("/")[0];
				String dd = date.split("/")[1];
				String D = m+"/"+dd+"/"+yy;
				
				String time = timestamp.split(" ")[1];
				String hh = time.split(":")[0];
				String mm = time.split(":")[1];
				
				int minnum = Integer.parseInt(mm)/5;		
				
				String name = nodes[1].trim();
				
				if (name.length()>32)
				{
					name = name.substring(0,32);
				}
				
				if (nodes.length>11)
				{
					context.write(NullWritable.get(),new Text(name+","+D+","+hh+","+Integer.toString(minnum)+","+nodes[11].trim()+","+nodes[8].trim()+","+nodes[10].trim()+","+"0"));
					
				}
				else
				{
					context.write(NullWritable.get(),new Text(name+","+D+","+hh+","+Integer.toString(minnum)+","+"0"+","+"0"+","+"0"+","+"0"));
					
				}
											
					
			}
			
							
		} // End method "map"
		
	} // End Class Map_One
	
	
 	
}
 	
 	
 	
	


