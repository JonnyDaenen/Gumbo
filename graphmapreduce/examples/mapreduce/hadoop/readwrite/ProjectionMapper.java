package mapreduce.hadoop.readwrite;

// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import mapreduce.guardedfragment.planner.structures.data.Tuple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProjectionMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private static final int MISSING = 9999;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {


		//System.err.println(key.get() + ":" + value.toString());
		
		Tuple t = new Tuple(value.toString());
		

	}
}
// ^^ MaxTemperatureMapper
