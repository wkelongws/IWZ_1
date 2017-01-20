import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class test extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new test(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		int numarg = args.length;
		String input = args[0];
		
		System.out.println("WATCH HERE!!!!!");
		
		System.out.println(input);
		System.out.println(numarg);
		
		for (String n:args){System.out.println(n);}
		
		return 0;
	}
}