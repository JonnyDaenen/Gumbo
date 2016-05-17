package mapreduce.maxtemp;

// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//System.err.println(key.get() + ":" + value.toString());
		context.write(key, value);
		
	}
}
// ^^ MaxTemperatureMapper
