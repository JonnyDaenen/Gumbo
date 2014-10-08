package mapreduce.hadoop.readwrite;

// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
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
		
		if(vals.length == 0 )
			return;
		
		String line = vals[vals.length-1];
		String year = line.substring(15, 19);
		int airTemperature;
		

		
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus
										// signs
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			// System.out.println(year);
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
}
// ^^ MaxTemperatureMapper
