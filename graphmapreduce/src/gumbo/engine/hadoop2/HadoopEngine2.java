package gumbo.engine.hadoop2;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.ExecutionException;
import gumbo.engine.general.grouper.GroupingException;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.hadoop2.converter.MultiRoundConverter;

public class HadoopEngine2 {


	private static final Log LOG = LogFactory.getLog(HadoopEngine2.class);
	private static final int WAIT = 1000;


	private JobControl jc;
	private GumboPlan plan;
	private Configuration conf;



	/**
	 * Executes a MR plan using Hadoop.
	 */
	public void executePlan (GumboPlan plan, Configuration conf) throws ExecutionException {

		this.plan = plan;
		this.conf = conf;

		execute();
		cleanup();

	}

	private void cleanup() {
		// TODO remove all tmp dirs

	}

	private void initJobControl(GumboPlan plan) {

		LOG.info("Creating Job Control for: " + plan.getName());
		jc = new JobControl(plan.getName());

		// 2. we execute the jobcontrol in a Thread
		Thread workflowThread = new Thread(jc, "Gumbo-Workflow-Thread_" + plan.getName());
		workflowThread.setDaemon(true); // will not avoid JVM to shutdown
		LOG.info("Starting Job-control thread: " + workflowThread.getName());
		workflowThread.start();
	}

	private void shutdownJobControl() {
		if (jc.getFailedJobList().size() > 0) {
			LOG.error(jc.getFailedJobList().size() + " jobs failed!");

			for (ControlledJob job : jc.getFailedJobList()) {
				LOG.error("\t" + job.getJobName() + " failed.");
			}
		} else {
			LOG.info("SUCCESS: all jobs (" + jc.getSuccessfulJobList().size() + ") completed!");

		}

		LOG.info("Stopping job control");
		jc.stop();
	}


	/**
	 * Starts a jobcontrol, executes jobs and shuts down the jobcontrol.
	 * @param plan
	 * @param conf
	 */
	private void execute() {

		initJobControl(plan);

		try {
			long start = System.nanoTime();
			createAndExecuteJobs(plan, plan.getPartitions());
			long stop = System.nanoTime();
			LOG.info("Running time: " + (stop-start)/1000000 + "ms");
		} catch (InterruptedException e) {
			LOG.error("Plan execution was interrupted!");
			e.printStackTrace();
		}

		shutdownJobControl();

	}

	/**
	 * Executes partitions bottom up.
	 * Each partition is converted to hadoop jobs and and executed.
	 * The way the jobs are converted depends on the grouping strategy used.
	 * @param plan 
	 * 
	 * @param partitions
	 * @throws InterruptedException 
	 */
	private void createAndExecuteJobs(GumboPlan plan, PartitionedCUGroup partitions) throws InterruptedException {

		try {
			MultiRoundConverter converter = new MultiRoundConverter(plan, conf);

			for (CalculationUnitGroup partition : partitions.getBottomUpList()) {

				// FUTURE start next level as soon as possible


				// check if this partition is eligible for 1-round conversion
				if (converter.is1Round(partition)) {
					LOG.info("Using 1-round evaluation for " + partitions);
					List<ControlledJob> jobs1Round = converter.createValEval(partition);
					for (ControlledJob job: jobs1Round) {
						jc.addJob(job);
					}

					// if not, split into 2 rounds
				} else {
					LOG.info("Using 2-round evaluation for " + partitions);

					// perform grouping on the rest
					List<CalculationGroup> groups = converter.group(partition);


					LOG.info("Starting round 1 (VAL)");
					// create and execute all round 1 jobs
					for (CalculationGroup group : groups) {
						ControlledJob job = converter.createValidateJob(group);
						jc.addJob(job);
					}

					// wait for completion
					waitForJC();

					LOG.info("Starting round 2 (EVAL)");
					// create and execute round 2 job
					List<ControlledJob> jobs = converter.createEvaluateJob(partition);
					for (ControlledJob job: jobs) {
						jc.addJob(job);
					}
				}

				// wait for completion
				waitForJC();


				converter.moveOutputFiles(partition);


			}
		} catch (IOException | GroupingException e) {
			throw new InterruptedException(e.getMessage());
		}

	}

	private void waitForJC() throws InterruptedException{
		LOG.info("Waiting for jobs");
		while (!jc.allFinished()){
			Thread.yield();
			Thread.sleep(WAIT);
		}
		LOG.info("Jobs are done.");
	}



}
