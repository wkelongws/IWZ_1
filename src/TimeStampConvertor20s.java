
import java.io.IOException;
import java.util.*;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class TimeStampConvertor20s extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
				
		Object value1 = input.get(0);
		Object value2 = input.get(1);
		Object value3 = input.get(2);
		
		String date = (String)value1;
		int hour = (int)value2;
		
		
		String minu = (String)value3;
		
		String h=Integer.toString(hour);
		if (hour<10){h = "0"+h;}
		
		String timestamp = date + " " + h + ":" + minu;
		return timestamp;
		
		
	}
}
