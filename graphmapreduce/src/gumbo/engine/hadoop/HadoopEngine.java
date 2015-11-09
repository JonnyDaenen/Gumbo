/**
 * Created: 05 Feb 2015
 */
package gumbo.engine.hadoop;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.general.ExecutionException;
import gumbo.engine.hadoop.converter.GumboHadoopConverter;
import gumbo.engine.hadoop.converter.GumboHadoopConverter.ConversionException;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * 
 * Runs a set of  jobs, which converted from abstract jobs to hadoop jobs at runtime.
 * This runtime creation allows for just-in-time estimation of input characteristics, 
 * which can be used to estimate the number of reducers.
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class HadoopEngine {


	private static final long REFRESH_WAIT = 500; // FUTURE increase to 5000ms
	private static final Log LOG = LogFactory.getLog(HadoopEngine.class);
	private String stats;


	public HadoopEngine() {
	}

	/**
	 * Executes a MR plan using Hadoop.
	 * @param plan a MR plan
	 * @param conf 
	 * @throws ExecutionException 
	 */
	public void executePlan (GumboPlan plan, Configuration conf) throws ExecutionException {

			
		GumboHadoopConverter jobConverter = new GumboHadoopConverter(plan.getName(), plan.getFileManager(), conf);

		executeJobs(plan, jobConverter);
		cleanup();

	}




	private void cleanup() {
		// TODO Auto-generated method stub

	}


	/**
	 * Executes a Gumbo Query and returns whether the execution was successful.
	 * Partitions are converted to hadoop {@link ControlledJob}s and fed to a {@link JobControl} instance
	 * one by one, to ensure that runtime adjustments can be made. Adjustments include for example the
	 * estimation of the number of reduce jobs, based on the input size.
	 * 
	 * @param plan a Gumbo plan
	 * @param jobConverter a converter
	 * 
	 * @return <code>true</code> when all jobs have finished successfully, <code>false</code> otherwise
	 * 
	 * @throws ExecutionException when the execution of the plan fails
	 */
	private boolean executeJobs(GumboPlan plan, GumboHadoopConverter jobConverter) throws ExecutionException {

		boolean success = false;
		stats = "";
		long start = System.nanoTime();

		try {

			// 1. create JobControl
			LOG.info("Creating Job Control for: " + plan.getName());
			JobControl jc = new JobControl(plan.getName());



			// 2. we execute the jobcontrol in a Thread
			Thread workflowThread = new Thread(jc, "Gumbo-Workflow-Thread_" + plan.getName());
			workflowThread.setDaemon(true); // will not avoid JVM to shutdown
			LOG.info("Starting Job-control thread: " + workflowThread.getName());
			workflowThread.start();

			// 3. execute jobs

			// for each partition from bottom to top
			HadoopPartitionQueue queue = new HadoopPartitionQueue(plan.getPartitions());

			LOG.info("Processing partition queue.");
			// while not all completed or the jobcontrol is still running
			while (!queue.isEmpty() || !jc.allFinished()) {

				// update states
				Set<CalculationUnitGroup> newPartitions = queue.updateStatus();


				//				LOG.info("Partitions to add: " + newPartitions.size());

				// for each job for which the dependencies are done
				// (and whose input files are now available for analysis)
				for (CalculationUnitGroup partition: newPartitions) {

					// convert job to hadoop job using only files 
					// (glob and directory paths are converted)
					List<ControlledJob> jobs = jobConverter.convert(partition);

					// add all the controlled jobs to the job control
					jc.addJobCollection(jobs);

					// update the queue
					queue.addJobs(partition, jobs.get(0), jobs.get(1));

				}

				// sleep
				Thread.sleep(REFRESH_WAIT);
			}
			LOG.info("Partition queue exhausted.");


			long stop = System.nanoTime();

			// 4. remove intermediate data

			// TODO implement removal of intermediate data

			// 5. report outcome
			if (jc.getFailedJobList().size() > 0) {
				LOG.error(jc.getFailedJobList().size() + " jobs failed!");

				for (ControlledJob job : jc.getFailedJobList()) {
					LOG.error("\t" + job.getJobName() + " failed.");
				}

				// TODO print out relations that failed
			} else {
				success = true;
				LOG.info("SUCCESS: all jobs (" + jc.getSuccessfulJobList().size() + ") completed!");

			}
			

			System.out.println("Running time: " + (stop-start)/1000000 + "ms");
			
			stats = collectCounters(jc);
//			printCounters(jc);
//			printJobDAG(jc);


			jc.stop();
		} catch (ConversionException  | InterruptedException | IOException e) {
			LOG.error("Exception during Gumbo plan execution: " + e.getMessage());
			throw new ExecutionException("Execution failed: " + e.getMessage(), e);
		}


		return success;

	}
	
	public String getCounters() {
		return stats;
	}
	
	private String collectCounters(JobControl jc) throws IOException {

		//		if (jc.getSuccessfulJobList().size() == 0)
		//			return;

		// initialize overall counters
		Counters overallCounters = new Counters();
		StringBuilder sb = new StringBuilder(20000);

		for (ControlledJob job : jc.getSuccessfulJobList()) {
			sb.append("Counters for job: " + job.getJobName());
			sb.append(System.lineSeparator());
			sb.append("--------------------------------------------------------------------------------");
			sb.append(System.lineSeparator());
			Counters counters = job.getJob().getCounters();


			for (String groupName : counters.getGroupNames()) {

				if (!groupName.contains("."))
					continue;

				CounterGroup group = counters.getGroup(groupName);

				sb.append(group.getDisplayName());
				sb.append(System.lineSeparator());
				sb.append(groupName);
				sb.append(System.lineSeparator());

				// aggregate counters
				CounterGroup overallGroup = overallCounters.getGroup(group.getName());
				//				CounterGroup overallGroup = overallCounters.addGroup(group.getName(), group.getDisplayName());


				for (Counter counter : group.getUnderlyingGroup()) {

					sb.append("\t" + counter.getDisplayName() + "=" + counter.getValue());
					sb.append(System.lineSeparator());

					// aggregate counters
					Counter overallCounter = overallGroup.findCounter(counter.getName(), true);
					//					Counter overallCounter = overallGroup.addCounter(counter.getName(), counter.getDisplayName(), 0);
					overallCounter.increment(counter.getValue());

				}

			}

		}


		Counters counters = overallCounters;


		sb.append(System.lineSeparator());
		sb.append("Overall Counters");
		sb.append(System.lineSeparator());
		sb.append("--------------------------------------------------------------------------------");
		sb.append(System.lineSeparator());
		for (String groupName : counters.getGroupNames()) {

			if (!groupName.contains("org.apache.hadoop.mapreduce"))
				continue;

			CounterGroup group = counters.getGroup(groupName);
			sb.append(group.getDisplayName());
			sb.append(System.lineSeparator());

			for (Counter counter : group.getUnderlyingGroup()) {
				sb.append("\t" + counter.getDisplayName() + "=" + counter.getValue() );
				sb.append(System.lineSeparator());
			}
		}

		return sb.toString();

	}

	/**
	 * source: http://www.slideshare.net/martyhall/hadoop-tutorial-mapreduce-part-5-mapreduce-features 
	 */
	private void printCounters(JobControl jc) throws IOException {

		//		if (jc.getSuccessfulJobList().size() == 0)
		//			return;

		// initialize overall counters
		Counters overallCounters = new Counters();

		for (ControlledJob job : jc.getSuccessfulJobList()) {
			System.out.println();
			System.out.println("Counters for job: " + job.getJobName());
			Counters counters = job.getJob().getCounters();


			for (String groupName : counters.getGroupNames()) {

				if (!groupName.contains("."))
					continue;

				CounterGroup group = counters.getGroup(groupName);
				System.out.println(group.getDisplayName());
				System.out.println(groupName);

				// aggregate counters
				CounterGroup overallGroup = overallCounters.getGroup(group.getName());
				//				CounterGroup overallGroup = overallCounters.addGroup(group.getName(), group.getDisplayName());


				for (Counter counter : group.getUnderlyingGroup()) {
					System.out.println("\t" + counter.getDisplayName() + "=" + counter.getValue() );

					// aggregate counters
					Counter overallCounter = overallGroup.findCounter(counter.getName(), true);
					//					Counter overallCounter = overallGroup.addCounter(counter.getName(), counter.getDisplayName(), 0);
					overallCounter.increment(counter.getValue());

				}

			}

		}


		Counters counters = overallCounters;


		System.out.println();
		System.out.println("Overall Counters");
		for (String groupName : counters.getGroupNames()) {

			if (!groupName.contains("org.apache.hadoop.mapreduce"))
				continue;

			CounterGroup group = counters.getGroup(groupName);
			System.out.println(group.getDisplayName());

			for (Counter counter : group.getUnderlyingGroup()) {
				System.out.println("\t" + counter.getDisplayName() + "=" + counter.getValue() );
			}
		}


	}

	private void printJobDAG(JobControl jc) {

		System.out.println();
		for (ControlledJob job : jc.getSuccessfulJobList()) {
			printJob(job);
		}
		System.out.println();

	}

	/**
	 * @param job
	 */
	private void printJob(ControlledJob job) {
		String depstring = "";
		if (job.getDependentJobs() != null) { 
			for (ControlledJob dep : job.getDependentJobs()) {
				depstring += ", " + dep.getMapredJobId();
			}
			if (!depstring.isEmpty()) {
				depstring = depstring.substring(1);
				depstring = " ->" + depstring;
			}
		}
		System.out.println(job.getMapredJobId() + " " + depstring);

	}


}
