/**
 * Created: 22 Aug 2014
 */
package gumbo.compiler.structures;

import gumbo.compiler.structures.operations.GFMapper;
import gumbo.compiler.structures.operations.GFReducer;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

/**
 * A 1-round MR-job containing: - id - name - input files - output files -
 * dependencies - map function - reduce function
 * 
 * @author Jonny Daenen
 * 
 */
public class MRJob {
	
	/**
	 * @author jonny
	 *
	 */
	public enum MRJobType {
		GF_ROUND1,
		GF_ROUND2
	}

	// TODO improve
	public static class IdGenerator {
		  private static int id = 0;
		  public static synchronized int generate() { return id++; }
		}


	String name;
	long id; 
	boolean outputJob;

	Set<MRJob> dependencies;
	Set<Path> inPaths;
	Path outFolder;

	Class<? extends GFMapper> mapFunction;
	Class<? extends GFReducer> reduceFunction;

	Set<GFExistentialExpression> expressions;

	private MRJobType type;

	/**
	 * @param name
	 *            the name of the MR-job.
	 */
	public MRJob(String name) {
		this.name = name;
		this.id = IdGenerator.generate();

		dependencies = new HashSet<MRJob>();
		inPaths = new HashSet<Path>();
		outputJob = true;
	}

	/**
	 * Add a dependency.
	 * 
	 * @param job
	 *            a job on which the current job depends
	 */
	public void addDependingJob(MRJob job) {
		dependencies.add(job);

	}

	/**
	 * Set the locations of the input, folders of files.
	 * 
	 * @param in
	 *            set of folders and/or files
	 */
	public void addInputPaths(Set<Path> in) {
		inPaths.addAll(in);
	}

	/**
	 * @param outFolder
	 *            the output folder
	 */
	public void setOutputPath(Path outFolder) {
		this.outFolder = outFolder;

	}

	/**
	 * @param set
	 */
	public void setExpressions(Set<GFExistentialExpression> set) {
		this.expressions = set;

	}

	/**
	 * @param mapFunction
	 *            the mapFunction to set
	 */
	public void setMapFunction(Class<? extends GFMapper> mapFunction) {
		this.mapFunction = mapFunction;
	}

	/**
	 * @param reduceFunction
	 *            the reduceFunction to set
	 */
	public void setReduceFunction(Class<? extends GFReducer> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		
		// OPTIMIZE use StringBuilder

		String output = "";
		// jobid will be unassigned in beginning
		output += "Job: " + name;

		output += System.getProperty("line.separator");
		output += "Input paths: " + inPaths;
		output += System.getProperty("line.separator");

		output += "Output path: " + outFolder;
		output += System.getProperty("line.separator");

		String deps = "";
		for (MRJob dep : dependencies) {
			deps += "," + dep.getJobName();
		}
		output += "Depending on: {" + deps.substring(Math.min(1,deps.length())) + "}";
		
		String epxs = "";
		for ( GFExistentialExpression e : expressions) {
			epxs += expressions.toString();
		}
		output += System.getProperty("line.separator");
		output += "Contributing to: {" + epxs.substring(Math.min(1,epxs.length())) + "}";

		output += System.getProperty("line.separator");
		output += System.getProperty("line.separator");
		
		return output;
	}

	/**
	 * @return the name of the job
	 */
	public String getJobName() {
		return name;
	}

	/**
	 * @return the id of the job
	 */
	public long getJobId() {
		return id;
	}

	/**
	 * @return the jobs the current job depends on
	 */
	public Set<MRJob> getDependencies() {
		return dependencies;
	}
	
	/**
	 * Removes the dependencies that can be reached indirectly.
	 */
	public void removeUnnecessaryDependencies() {
		Collection<MRJob> reachables = getDepencencyClosureLevel1();
		dependencies.removeAll(reachables);
	}
	
	/**
	 * @return the set of dependencies that can be reached from this node.
	 */
	private Collection<MRJob> getDepencencyClosure(){
		Set<MRJob> deps = new HashSet<MRJob>();
		deps.addAll(dependencies);
		for(MRJob job : dependencies) {
			deps.addAll(job.getDepencencyClosure());
		}
		return deps;
	}
	
	/**
	 * @return the dependencies that can be reached indirectly
	 */
	private Collection<MRJob> getDepencencyClosureLevel1(){
		Set<MRJob> deps = new HashSet<MRJob>();
		for(MRJob job : dependencies) {
			deps.addAll(job.getDepencencyClosure());
		}
		return deps;
	}

	/**
	 * @return the collection of input paths
	 */
	public Collection<Path> getInputPaths() {
		return inPaths;
	}

	/**
	 * @return the output path
	 */
	public Path getOutputPath() {
		return outFolder;
	}
	
	/**
	 * @return the expressions
	 */
	public Collection<GFExistentialExpression> getGFExpressions() {
		return expressions;
	}


	/**
	 * @return the class of the mapper.
	 */
	public Class<? extends GFMapper> getMapClass() {
		return mapFunction;
	}

	/**
	 * @return the class of the reducer.
	 */
	public Class<? extends GFReducer> getReduceClass() {
		return reduceFunction;
	}
	
	/**
	 * Indicate whether the output of this job is important or temporary.
	 * @param outputJob whether the results of this job should be part of the final output
	 */
	public void setOutputJob(boolean outputJob) {
		this.outputJob = outputJob;
	}
	
	/**
	 * @return true if the job output should be kept, false if the output is temporary
	 */
	public boolean isOutputJob() {
		return outputJob;
	}
	
	/**
	 * Sets the type of the MR job. The types indicates the functionality of the job.
	 */
	public void setType(MRJobType type) {
		this.type = type;
	}

	/**
	 * @return the type indicating the functionality of the job
	 */
	public MRJobType getType() {
		return type;
	}

	
}
