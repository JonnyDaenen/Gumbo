package mapreduce.maxtemp;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainedDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: MaxTemperatureDriver <input path> <outputpath>");
			System.exit(-1);
		}

		boolean success = false;

		String in = args[0];
		String out = args[1] + "/out";
		String tmpdir = args[1] + "/data-" + getClass().getSimpleName()
				+ "-tmp";
		Path tmpDir = new Path(tmpdir);
		deleteTmp(tmpDir);

		try {
			Job job1 = createJob1(in, tmpdir);
			Job job2 = createJob2(tmpdir, out);

			// 1. create JobControl
			JobControl jc = new JobControl("TestGroup");

			// 2. For each job, we create a controlled job
			ControlledJob cjob1 = new ControlledJob(job1, null);
			ControlledJob cjob2 = new ControlledJob(job2, Arrays.asList(cjob1)); // depends
																					// on
																					// job1

			// 3. we add all the controlled jobs to the job control
			// note that this can also be done in 1 go by using a collection
			jc.addJob(cjob1);
			jc.addJob(cjob2);

			// 4. we execute the jobcontrol in a Thread
			Thread workflowThread = new Thread(jc, "Workflow-Thread");
			workflowThread.setDaemon(true); // TODO what's this?
			workflowThread.start();

			// 5. we wait for it to complete
			while (!jc.allFinished()) {
				Thread.sleep(500);
			}

			// 6. clean up in case of failure

			if (jc.getFailedJobList().size() > 0) {
				System.err.println(jc.getFailedJobList().size()
						+ " jobs failed!");

				for (ControlledJob job : jc.getFailedJobList()) {
					System.err.println("\t" + job.getJobName() + " failed.");
				}
			} else {
				success = true;
				System.err.println("SUCCESS: all jobs ("
						+ jc.getSuccessfulJobList().size() + ") completed!");
			}
			
		} finally {
			// remove the temp dir
			deleteTmp(tmpDir);
		}
		return success ? 0 : 1;
	}
	
	private void deleteTmp(Path intermediatePath)
			throws IOException {
//		FileSystem fs = FileSystem.get(getConf());
//		if (fs.exists(intermediatePath)){
//			fs.delete(intermediatePath, true);
//		}
	}

	public Job createJob1(String in, String out) throws IOException {

		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName("phase1");
		
		// set IO
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		
		
		// set mapper an reducer
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);

		// set intermediate output
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		TextOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setOutputFormatClass(TextValueOnlyOutputFormat.class);
		
		
		 
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);


		return job;
	}

	public Job createJob2(String in, String out) throws IOException {
		// Job job = new Job(new Configuration());
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName("phase2");

		// set I/O
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		// job.setInputPath(new Path(args[0]));
		// job.setOutputPath(new Path(args[1]));

		// configure mapper and reducer (and combiner if necessary)
		job.setMapperClass(MaxTemperatureMapper.class);
		// TODO setCombinerClass
		job.setReducerClass(MaxTemperatureReducer.class);

		// configure output
		TextOutputFormat.setOutputPath(job, new Path(out)); // TODO what's this?
		job.setOutputFormatClass(TextOutputFormat.class); // TODO what's this?
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		ChainedDriver driver = new ChainedDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
