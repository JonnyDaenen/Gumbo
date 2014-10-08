/**
 * Created: 06 Oct 2014
 */
package mapreduce.hadoop.readwrite;

import mapreduce.maxtemp.MaxTemperatureDriver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Jonny Daenen
 *
 */
public class HadoopCustomReader extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureDriver <input path> <outputpath>");
			System.exit(-1);
		}

		//Job job = new Job(new Configuration());
		Job job = Job.getInstance();
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max Temperature");

		FileInputFormat.addInputPath(job, new Path("input/q4/1e04/*.rel"));
		String output = "./output/" + this.getClass().getSimpleName() + "/" + System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(output));
		
//		job.setInputPath(new Path(args[0]));
//	    job.setOutputPath(new Path(args[1]));

		job.setMapperClass(ProjectionMapper.class);
		job.setReducerClass(ProjectionReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {
		HadoopCustomReader driver = new HadoopCustomReader();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
