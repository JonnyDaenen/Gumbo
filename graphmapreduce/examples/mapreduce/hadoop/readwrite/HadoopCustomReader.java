/**
 * Created: 06 Oct 2014
 */
package mapreduce.hadoop.readwrite;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapreduce.maxtemp.MaxTemperatureDriver;

/**
 * @author Jonny Daenen
 *
 */
public class HadoopCustomReader extends Configured implements Tool{

	public int run(String[] args) throws Exception {
	

		//Job job = new Job(new Configuration());
		Job job = Job.getInstance();
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("CustomReader");

		FileInputFormat.addInputPath(job, new Path("input/dummyrelations1/"));
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		String output = "./output/" + this.getClass().getSimpleName() + "/" + timeStamp;
		FileOutputFormat.setOutputPath(job, new Path(output));
		
//		job.setInputPath(new Path(args[0]));
//	    job.setOutputPath(new Path(args[1]));
		
		job.setInputFormatClass(RelationInputFormat.class);

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
