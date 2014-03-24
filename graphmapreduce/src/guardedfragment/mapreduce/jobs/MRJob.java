package guardedfragment.mapreduce.jobs;

import guardedfragment.data.RelationSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

public abstract class MRJob {

	String inputDir;
	String outputDir;
	
	Set<RelationSchema> relations;
	Set<MRJob> dependencies;
	
	private long id;
	private String name;
	
	public MRJob(long id, String name) {
		this.id = id;
		this.name = name;
		
		dependencies = new HashSet<MRJob>();
		relations = new HashSet<RelationSchema>();
	}

	public String getInputDir() {
		return inputDir;
	}

	public void setInputDir(String inputDir) {
		this.inputDir = inputDir;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}
	

	public void addInputRelation(RelationSchema s) {
		relations.add(s);
	}

	public void addDependency(MRJob job) {
		dependencies.add(job);
	}

	public ControlledJob createHadoopJob() throws IOException {

		Set<ControlledJob> result = new HashSet<ControlledJob>();

		// create new job
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName("job-"+id+"-"+name);

		// hook up dependencies
		ArrayList<ControlledJob> deps = new ArrayList<ControlledJob>(result);
		ControlledJob currentJob = new ControlledJob(job, deps);

		// add all dependencies to set
		for (MRJob dependingJob : dependencies) {
			currentJob.addDependingJob(dependingJob.createHadoopJob());
		}
		// FIXME same MRJobs yield different hadoop jobs

		// TODO set mapper

		// TODO set reducer

		return currentJob;
	}
}
