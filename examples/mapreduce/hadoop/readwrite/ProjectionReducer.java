package mapreduce.hadoop.readwrite;

// cc MaxTemperatureReducer Reducer for maximum temperature example
// cc MaxTemperatureReducer2 Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProjectionReducer extends
		Reducer<Text, Text, Text, Text> {


	/**
	 * @see mapreduce.hadoop.readwrite.ProjectionReducer#reduce(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for (IntWritable value : values) {
			count++;
		}
		context.write(key, count);
	}
}
