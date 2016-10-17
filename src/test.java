import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String line = "I-80 CBDS 57 EB Off Ramp to S Expressway Street";
		System.out.println(line.length());
		if (line.length()>32)
		{
			System.out.println(line.substring(0,32));
		}
		else
		{
			System.out.println(line.trim());
		}
		
		String[] nodes = line.split(",");
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
		
		for (int i=0;i<nodes.length;i++)
		{
			System.out.println(nodes[i]);
		}
		//System.out.println(nodes[1].trim()+","+D+","+hh+","+Integer.toString(minnum)+","+nodes[11].trim()+","+nodes[8].trim()+","+nodes[10].trim()+","+"0");
		
	}

}
