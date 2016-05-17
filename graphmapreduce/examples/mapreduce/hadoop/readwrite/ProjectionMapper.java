package mapreduce.hadoop.readwrite;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import gumbo.structures.data.Tuple;

public class ProjectionMapper extends
		Mapper<Text, Text, Text, Text> {

	private static final int MISSING = 9999;

	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {


		//System.err.println(key.get() + ":" + value.toString());
		
		System.out.println(key.toString() + ": " + value.toString());
		
		Tuple t = new Tuple(value.toString());
		
		context.write(new Text(t.getName()), new Text(t.toString()));

	}
}
// ^^ MaxTemperatureMapper
