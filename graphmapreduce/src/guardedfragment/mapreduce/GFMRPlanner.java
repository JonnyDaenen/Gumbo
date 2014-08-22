package guardedfragment.mapreduce;

import guardedfragment.mapreduce.mappers.GuardedBooleanMapper;
import guardedfragment.mapreduce.mappers.GuardedMapper;
import guardedfragment.mapreduce.planner.GFMRPlannerException;
import guardedfragment.mapreduce.planner.structures.MRPlan;
import guardedfragment.mapreduce.reducers.GuardedAppearanceReducer;
import guardedfragment.mapreduce.reducers.GuardedProjectionReducer;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Converter from a GF expression to a Map-reduce ControlledJob (hadoop). For
 * now, only basic GR expressions are allowed.
 * 
 * Configuration: - scratch dir - input dir - output dir
 * 
 * Tasks: 
 * - create jobs with correct mapper and reducer 
 * - set job dependencies 
 * - determine intermediate output (tmp) folders 
 * - link output to input of dependent jobs 
 * - TODO basic clean up of tmp folders 
 * - TODO clean up tmp folders as soon as possible
 * 
 * @author Jonny Daenen
 * 
 * 
 */
@Deprecated
public class GFMRPlanner {

	protected Path inputDir;
	protected Path outputDir;
	protected Path scratchDir;

	protected String planName;
	private int jobCounter;
	static final String TMPDIRPREFIX = "TMP_JOB";
	
	private GFPrefixSerializer serializer;
	


	private static final Log LOG = LogFactory.getLog(GFMRPlanner.class);
	

	public GFMRPlanner(String inputDir, String outputDir, String scratchDir) {
		super();
		
		this.inputDir = new Path(inputDir);
		this.outputDir = new Path(outputDir);
		this.scratchDir = new Path(scratchDir);
		this.planName = "";
		this.jobCounter = 0;
		
		serializer = new GFPrefixSerializer();
	}

	MRPlan convert(GFExpression e) throws GFMRPlannerException {
		
//		Set<GFExpression> set = new HashSet<GFExpression>();
//		set.add(e);
//		return convert(set);
		
		MRPlan plan = new MRPlan();
		// set basic properties
		plan.setInputFolder(inputDir);
		plan.setOutputFolder(outputDir);
		plan.setScratchFolder(scratchDir);
		
		// TODO implement
		
		
		return plan;
	}
	
//	MRPlan convert(Set<GFExpression> set) {
//		// TODO implement
//		return null;
//	}

	
	MRPlan convert(Set<GFExistentialExpression> set)
			throws GFMRPlannerException, IOException {

		MRPlan plan = new MRPlan();
		
		Path tmpDir = getTmpDir("" + jobCounter);

		ControlledJob phase1job = createBasicGFRound1Job(inputDir, tmpDir, set);
		ControlledJob phase2job = createBasicGFRound2Job(tmpDir, outputDir, set);
		phase2job.addDependingJob(phase1job);

		// add jobs to plan
		plan.addJob(phase1job);
		plan.addJob(phase2job);
			
		// TODO is this necessary?
		plan.addTempDir(tmpDir);
		plan.setInputFolder(inputDir);
		plan.setOutputFolder(outputDir);

		return plan;
	}

	private Path getTmpDir(String jobName) {
		return new Path(scratchDir.toString() + "/" + TMPDIRPREFIX + jobName);
	}
	

	
	
	/**
	 * Creates a round 1 MR-job for a SET of basic existential GFExpressions.
	 * 
	 * @param in input folder
	 * @param out output folder
	 * @param e GFExistentialExpression
	 * @return a ControlledJob, configured properly
	 * @throws IOException
	 */
	private ControlledJob createBasicGFRound1Job(Path in, Path out, Set<GFExistentialExpression> set)
			throws IOException {

		// create basic job
		Job job = createJob(in, out);

		// set mapper an reducer
		job.setMapperClass(GuardedMapper.class);
		job.setReducerClass(GuardedAppearanceReducer.class);
		
		// set guard and guarded set
		Configuration conf = job.getConfiguration();
		conf.set("formulaset", serializer.serializeSet(set));

		// set intermediate/mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer output
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return new ControlledJob(job, null);
	}
	
	/**
	 * Creates a round 2 MR-job for a SET of basic existential GFExpression.
	 * 
	 * @param in input folder
	 * @param out output folder
	 * @param guard 
	 * @param booleanformula 
	 * @return a ControlledJob, configured properly
	 * @throws IOException
	 */
	private ControlledJob createBasicGFRound2Job(Path in, Path out, Set<GFExistentialExpression> set) throws IOException {
		

		// create basic job
		Job job = createJob(in, out);

		// set mapper an reducer
		job.setMapperClass(GuardedBooleanMapper.class);
		job.setReducerClass(GuardedProjectionReducer.class);
		
		
		Configuration conf = job.getConfiguration();
		conf.set("formulaset", serializer.serializeSet(set));

		// set intermediate/mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer output
		job.setOutputKeyClass(NullWritable.class);
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
		job.setJobName("job" + jobCounter++); 

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



}
