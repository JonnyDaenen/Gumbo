package guardedfragment.mapreduce;

import java.io.IOException;

import mapreduce.MRPlan;
import mapreduce.maxtemp.IdentityMapper;
import mapreduce.maxtemp.IdentityReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import guardedfragment.mapreduce.jobs.BooleanMRJob;
import guardedfragment.mapreduce.jobs.MRJob;
import guardedfragment.mapreduce.mappers.GuardedBooleanMapper;
import guardedfragment.mapreduce.mappers.GuardedMapper;
import guardedfragment.mapreduce.reducers.GuardedAppearanceReducer;
import guardedfragment.mapreduce.reducers.GuardedProjectionReducer;
import guardedfragment.structure.GFAndExpression;
import guardedfragment.structure.GFExistentialExpression;
import guardedfragment.structure.GFExpression;
import guardedfragment.structure.GFNotExpression;
import guardedfragment.structure.GFOrExpression;

/**
 * Converter from a GF expression to a Map-reduce ControlledJob (hadoop). For
 * now, only basic GR expressions are allowed.
 * 
 * Configuration: - scratch dir - input dir - output dir
 * 
 * Tasks: - create jobs with correct mapper and reducer - set job dependencies -
 * determine intermediate output (tmp) folders - link output to input of
 * dependent jobs - TODO basic clean up of tmp folders - TODO clean up tmp
 * folders as soon as possible
 * 
 * @author Jonny Daenen
 * 
 * 
 */
public class GFMRPlanner {

	protected Path inputDir;
	protected Path outputDir;
	protected Path scratchDir;

	protected String planName;
	private int jobCounter;
	static final String TMPDIRPREFIX = "TMP_JOB";

	public GFMRPlanner(String inputDir, String outputDir, String scratchDir) {
		super();
		this.inputDir = new Path(inputDir);
		this.outputDir = new Path(outputDir);
		this.scratchDir = new Path(scratchDir);
		this.planName = "";
		this.jobCounter = 0;
	}

	MRPlan convert(GFExpression e) throws ConversionNotImplementedException {
		throw new ConversionNotImplementedException();
	}

	MRPlan convert(GFExistentialExpression e)
			throws ConversionNotImplementedException, IOException {

		MRPlan plan = new MRPlan();

		// if it is a basic existential expression, convert
		if (e.getChild().isAtomicBooleanCombination()) {

			Path tmpDir = getTmpDir("" + jobCounter);

			// Phase 1 job
			ControlledJob phase1job = createBasicGFPhase1Job(inputDir, tmpDir);

			// Phase 2 job, which depends on Phase 1 job
			ControlledJob phase2job = createBasicGFPhase2Job(tmpDir, outputDir);
			phase2job.addDependingJob(phase1job);

			// add jobs to plan
			plan.addJob(phase1job);
			plan.addJob(phase1job);

		} else
			throw new ConversionNotImplementedException();

		return plan;
	}

	private Path getTmpDir(String jobName) {
		return new Path(scratchDir.toString() + "/" + TMPDIRPREFIX + jobName);
	}

	/**
	 * Creates a phase 1 MR-job for a basic existential GFExpression.
	 * 
	 * @param in input folder
	 * @param out output folder
	 * @return a ControlledJob, configured properly
	 * @throws IOException
	 */
	private ControlledJob createBasicGFPhase1Job(Path in, Path out)
			throws IOException {

		// create basic job
		Job job = createJob(in, out);

		// set mapper an reducer
		job.setMapperClass(GuardedMapper.class);
		job.setReducerClass(GuardedAppearanceReducer.class);

		// set intermediate/mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer output
		job.setOutputKeyClass(null);
		job.setOutputValueClass(Text.class);

		return new ControlledJob(job, null);
	}

	/**
	 * Creates a phase 2 MR-job for a basic existential GFExpression.
	 * 
	 * @param in input folder
	 * @param out output folder
	 * @return a ControlledJob, configured properly
	 * @throws IOException
	 */
	private ControlledJob createBasicGFPhase2Job(Path in, Path out) throws IOException {
		// create basic job
		Job job = createJob(in, out);

		// set mapper an reducer
		job.setMapperClass(GuardedBooleanMapper.class);
		job.setReducerClass(GuardedProjectionReducer.class);

		// set intermediate/mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer output
		job.setOutputKeyClass(null);
		job.setOutputValueClass(Text.class);

		return new ControlledJob(job, null);
	}

	/**
	 * Creates a basic job with IO folders set correctly.
	 * 
	 * @param in
	 * @param out
	 * @throws IOException
	 * @returna job configured with IO folders
	 */
	private Job createJob(Path in, Path out) throws IOException {
		// create job
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName("job" + jobCounter++); // TODO improve

		// set IO
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job;
	}

	/* GETTERS & SETTERS */

	public String getInputDir() {
		return inputDir.toString();
	}

	public void setInputDir(String inputDir) {
		this.inputDir = new Path(inputDir);
	}

	public String getOutputDir() {
		return outputDir.toString();
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = new Path(outputDir);
	}

	public String getScratchDir() {
		return scratchDir.toString();
	}

	public void setScratchDir(String scratchDir) {
		this.scratchDir = new Path(scratchDir);
	}

	public String getPlanName() {
		return planName;
	}

	public void setPlanName(String planName) {
		this.planName = planName;
	}

	/*
	 * TODO add later MRJob convert(GFAndExpression e) throws
	 * ConversionNotImplementedException { return convertBoolean(e); }
	 * 
	 * MRJob convert(GFOrExpression e) throws ConversionNotImplementedException
	 * { return convertBoolean(e); }
	 * 
	 * MRJob convert(GFNotExpression e) throws ConversionNotImplementedException
	 * { return convertBoolean(e); }
	 * 
	 * BooleanMRJob convertBoolean(GFExpression e) throws
	 * ConversionNotImplementedException { if (e.isAtomicBooleanCombination())
	 * return null; // TODO new BooleanMRJob(); else
	 */

}
