/**
 * Created: 09 Feb 2015
 */
package gumbo.compiler.plan;

import gumbo.compiler.GumboPlan;

/**
 * @author Jonny Daenen
 *
 */
public interface PlanVisualizer {

	String visualize(GumboPlan gp);
}

//
//package mapreduce.guardedfragment.planner.structures;
//
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Set;
//
//import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//
///**
// * Provides a way to execute a set of MR jobs that are possibly dependent. All
// * jobs are executed in a seperate thread, which is polled for completion.
// * Input, output, scratch and temporary directories are to be specified. Scratch
// * and temp folders are removed after completion, unless suppressed with the
// * supplied preference.
// * 
// * The output and scratch folders should belong to one plan. The temp folders
// * should be located inside the scratch folder, but can later be used to provide
// * a more early temp folder deletion mechanism.
// * 
// * A summary of the plan is placed inside the output folder.
// * 
// * @author Jonny Daenen
// * 
// */
//public class MRPlan {
//
//	private static final Log LOG = LogFactory.getLog(MRPlan.class);
//
//	protected String name = "FronjoPlan"; // FUTURE change
//
//	protected RelationFileMapping inputPaths; // FUTURE I think this should be a set
//	protected Path outputFolder;
//	protected Path scratchFolder;
//
//	protected Set<MRJob> jobs;
//	protected Set<Path> tempdirs;
//	protected Set<Path> outdirs;
//
//	protected Set<RelationSchema> relations; // TODO add to description
//
//	boolean deleteTmpDirs;
//
//	public MRPlan() {
//		jobs = new HashSet<MRJob>();
//		tempdirs = new HashSet<Path>();
//		outdirs = new HashSet<Path>();
//		deleteTmpDirs = true;
//	}
//
//	/**
//	 * Add a job.
//	 * 
//	 * @param job
//	 *            the job to add
//	 */
//	public void addJob(MRJob job) {
//		jobs.add(job);
//	}
//
//	public void addAllJobs(Collection<MRJob> jobs) {
//		this.jobs.addAll(jobs);
//	}
//
//	public void addTempDir(Path tmpDir) {
//		tempdirs.add(tmpDir);
//	}
//
//	public void addTempDirs(Collection<Path> tmpDirs) {
//		tempdirs.addAll(tmpDirs);
//
//	}
//
//	public void addOutDirs(Collection<Path> outDirs) {
//		outdirs.addAll(outDirs);
//
//	}
//
//	/**
//	 * /* GETTERS & SETTERS
//	 */
//
//	/**
//	 * @param job
//	 * @return
//	 */
//	private String jobsummary(ControlledJob job) {
//		String output = "";
//		// jobid will be unassigned in beginning
//		output += "Job: " + job.getJobName();
//
//		output += System.getProperty("line.separator");
//		Path[] inputs1 = FileInputFormat.getInputPaths(new JobConf(job.getJob().getConfiguration()));
//		List<Path> inputs = Arrays.asList(inputs1);
//		output += "Input paths: " + inputs;
//		output += System.getProperty("line.separator");
//
//		output += "Output path: " + FileOutputFormat.getOutputPath(new JobConf(job.getJob().getConfiguration()));
//		output += System.getProperty("line.separator");
//
//		if (job.getDependentJobs() != null) {
//			String deps = "";
//			for (ControlledJob dep : job.getDependentJobs()) {
//				deps += "," + dep.getJobName();
//			}
//			output += "Depending on: {" + deps.substring(1) + "}";
//		}
//
//		output += System.getProperty("line.separator");
//
//		return output;
//	}
//
//	public RelationFileMapping getInputPaths() {
//		return inputPaths;
//	}
//
//	public void setInputPaths(RelationFileMapping infiles) {
//		this.inputPaths = infiles;
//	}
//
//	public Path getOutputFolder() {
//		return outputFolder;
//	}
//
//	public void setOutputFolder(Path outputFolder) {
//		this.outputFolder = outputFolder;
//	}
//
//	public Path getScratchFolder() {
//		return scratchFolder;
//	}
//
//	public void setScratchFolder(Path scratchFolder) {
//		this.scratchFolder = scratchFolder;
//	}
//
//	/**
//	 * Toggles deletion of tmp dirs after execution. When enabled, they are also
//	 * deleted when something has gone wrong.
//	 * 
//	 * @param deleteTmpDirs
//	 */
//	public void setDeleteTmpDirs(boolean deleteTmpDirs) {
//		this.deleteTmpDirs = deleteTmpDirs;
//	}
//
//	/**
//	 * @see java.lang.Object#toString()
//	 */
//	@Override
//	public String toString() {
//		String output = "";
//
//		// TODO add date
//
//		// name
//		output += System.getProperty("line.separator");
//		output += "MR-plan:";
//		output += System.getProperty("line.separator");
//		output += name;
//		output += System.getProperty("line.separator");
//
//		// jobs
//		output += System.getProperty("line.separator");
//		output += "Jobs:";
//		output += System.getProperty("line.separator");
//		output += "-----";
//		output += System.getProperty("line.separator");
//
//		for (MRJob job : jobs) {
//			output += job;
//		}
//
//		// folders
//		output += System.getProperty("line.separator");
//		output += "Folders:";
//		output += System.getProperty("line.separator");
//		output += "-------";
//		output += System.getProperty("line.separator");
//
//		output += "input: " + inputPaths;
//		output += System.getProperty("line.separator");
//		output += "output: " + outputFolder;
//		output += System.getProperty("line.separator");
//		output += "scratch: " + scratchFolder;
//		output += System.getProperty("line.separator");
//
//		output += "Output-dirs: ";
//		output += System.getProperty("line.separator");
//		for (Path dir : outdirs) {
//			output += "\t" + dir.toString();
//		}
//		output += System.getProperty("line.separator");
//
//		output += "Temp-dirs: ";
//		output += System.getProperty("line.separator");
//		for (Path dir : tempdirs) {
//			output += "\t" + dir.toString();
//		}
//		output += System.getProperty("line.separator");
//
//		return output;
//	}
//
//	/**
//	 * @return a schema of the plan in the dot language
//	 */
//	public String toDot() {
//		String output = "";
//		String sep = System.getProperty("line.separator");
//
//		// jobs
//		output += sep;
//		output += "# Jobs";
//		output += sep;
//		output += "# ----";
//		output += sep;
//
//		for (MRJob job : jobs) {
//			output += createNode("" + job.getJobId(), job.getJobName());
//		}
//
//		// folders
//		// output += sep
//		// output += "# Folders";
//		// output += sep
//		// output += "# ------";
//		// output += sep
//		//
//		// output += createNode("in",inputFolder.toString());
//		//
//		// for (Path dir : outdirs) {
//		// output += createNode("out",dir.toString());
//		// }
//		// for (Path dir : tempdirs) {
//		// output += createNode("tmp",dir.toString());
//		// }
//
//		// edges
//		output += sep;
//		output += "# Dependencies";
//		output += sep;
//		output += "# ------------";
//		output += sep;
//
//		for (MRJob job : jobs) {
//			for (MRJob e : job.getDependencies()) {
//				output += createEdge(job.getJobId(), e.getJobId());
//			}
//		}
//
//		output = "digraph " + name + "{ node [shape=record];" + output + "}";
//		return output;
//	}
//
//	/**
//	 * @param job
//	 * @return
//	 */
//	private String createNode(String id, String label) {
//		String output = "";
//		// output += "subgraph cluster_"+id+"{" +
//		// System.getProperty("line.separator");
//		// output += "style=filled;" + System.getProperty("line.separator");
//		// output += "color=lightgrey;" + System.getProperty("line.separator");
//		// output += "node [style=filled,color=white];" +
//		// System.getProperty("line.separator");
//		// output += "map"+id+"-> reduce" + id + ";" +
//		// System.getProperty("line.separator");
//		// output += "label = \""+label+"\"" +
//		// System.getProperty("line.separator");
//		// output += "}" + System.getProperty("line.separator");
//
//		output += id + " [label =\"" + label + "\"]";
//
//		return output + System.getProperty("line.separator");
//	}
//
//	private String createEdge(long fromId, long toId) {
//		return "" + fromId + " -> " + toId + System.getProperty("line.separator");
//	}
//
//	/**
//	 * @return a short descriptive name of the plan
//	 */
//	public String getName() {
//		return name;
//	}
//
//	/**
//	 * @return the jobs in the plan
//	 */
//	public Collection<MRJob> getJobs() {
//		return jobs;
//	}
//
//	/**
//	 * @return the set of intermediate directories
//	 */
//	public Set<Path> getTmpDirs() {
//		return tempdirs;
//	}
//
//	/**
//	 * @return the set of directories that correspond to new relations
//	 */
//	public Set<Path> getOutdirs() {
//		return outdirs;
//	}
//
//	/**
//	 * Indicates whether the temporary files and folders should be deleted after
//	 * execution.
//	 * 
//	 * @return true when the temporary files and folders should be deleted,
//	 *         false otherwise
//	 */
//	public boolean deleteTmpDirsEnabled() {
//		return deleteTmpDirs;
//	}
//
//	/**
//	 * Makes an iterable list of its jobs, while preserving dependencies. I.e., dependencies
//	 * are placed in front of the job. 
//	 * @return an iterable that gives all the jobs in a dependency-preserving order
//	 */
//	public Iterable<MRJob> getJobsLevelwise() {
//
//		Set<MRJob> currentLevel = Sets.newHashSet(getTopLevelJobs());
//		List<MRJob> list = new LinkedList<>();
//		
//		addToList(currentLevel, list);
//
//		// breadth first expansion
//		while (currentLevel.size() != 0) {
//
//			Set<MRJob> newLevel = new HashSet<>();
//
//			// expand current level
//			for (MRJob job : currentLevel) {
//
//				// add all the children to the new level
//				for (MRJob child : job.getDependencies()) {
//					newLevel.add(child);
//				}
//			}
//			
//
//			// add new level to the list, internal order does not matter
//			addToList(newLevel, list);
//
//			// level complete -> shift
//			currentLevel = newLevel;
//			
//
//		}
//		
//		LOG.debug(list.toString());
//
//		return Lists.reverse(list);
//
//	}
//
//	/**
//	 * @param newLevel
//	 * @param list
//	 */
//	private void addToList(Iterable<MRJob> newLevel, List<MRJob> list) {
//		for (MRJob mrJob : newLevel) {
//			list.add(mrJob);
//		}
//	}
//	
//	/**
//	 * 
//	 * @return the jobs that are flagged as output jobs
//	 */
//	public Iterable<MRJob> getOutputJobs() {
//		HashSet<MRJob> output = new HashSet<>();
//		
//		for (MRJob mrJob : this.jobs) {
//			if(mrJob.isOutputJob())
//				output.add(mrJob);
//		}
//		
//		return output;
//	}
//	
//	
//
//	/**
//	 * @return the set of jobs that do not appear as a dependency
//	 */
//	public Iterable<MRJob> getTopLevelJobs() {
//		Set<MRJob> jobs = new HashSet<>();
//
//		for (MRJob job : this.jobs) {
//			if (!isDependency(job))
//				jobs.add(job);
//		}
//
//		return jobs;
//
//	}
//
//	/**
//	 * Calculates the set of jobs that do not depend on other MRJobs
//	 * 
//	 * @return the set of jobs that have no dependencies
//	 */
//	public Iterable<MRJob> getBottomLevelJobs() {
//		Set<MRJob> jobs = new HashSet<>();
//
//		for (MRJob job : this.jobs) {
//			if (job.dependencies.size() == 0)
//				jobs.add(job);
//		}
//
//		return jobs;
//
//	}
//
//	/**
//	 * Checks whether a given job appears as a dependency of another job.
//	 * 
//	 * @param job
//	 *            the job to look up
//	 * @return true if there is another job dependent on the given job, false
//	 *         otherwise
//	 */
//	public boolean isDependency(MRJob job) {
//		for (MRJob pjob : jobs) {
//			if (pjob.dependencies.contains(job))
//				return true;
//		}
//		return false;
//	}
//}
