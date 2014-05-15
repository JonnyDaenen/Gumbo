/**
 * Created: 09 May 2014
 */
package guardedfragment.mapreduce.planner.compiler;

import guardedfragment.mapreduce.planner.calculations.CalculationUnit;
import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;
import guardedfragment.mapreduce.planner.partitioner.PartitionedCalculationUnitDAG;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapreduce.MRPlan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * Compiles Calculations in partitions into separate MR-jobs.
 * 
 * @author Jonny Daenen
 * 
 */
public class CalculationCompiler {

	int counter;
	DirManager dm;

	/**
	 * Creates a <code>MRPlan</code> that will execute the given calculations.
	 * 
	 * 
	 * @param partitionedDAG
	 *            the partitioned DAG of calculation units
	 * @param indir
	 *            directory containing files with data
	 * @param outdir
	 *            an empty directory to output the resulting relations
	 * @param scratchdir
	 *            an empty directory to be used as scratch space for
	 *            intermediate data
	 * 
	 * @throws CompilerException
	 * @throws UnsupportedCalculationUnitException
	 * 
	 * @see guardedfragment.mapreduce.planner.compiler.CalculationCompiler#compile(java.util.List)
	 */
	public MRPlan compile(PartitionedCalculationUnitDAG partitionedDAG, Path indir, Path outdir, Path scratchdir)
			throws UnsupportedCalculationUnitException, CompilerException {

		counter = 0;

		// for each partition we create a set of MR-jobs

		// map input and output relations to files
		dm = new DirManager(partitionedDAG, indir, outdir, scratchdir);
		
		// TODO global serializer
		// TODO change job name
		BasicGFCompiler compiler = new BasicGFCompiler(dm, new GFPrefixSerializer(), "Fronjo_job");


		// convert all to jobs
		Map<CalculationUnit, Set<ControlledJob>> jobmap = new HashMap<CalculationUnit, Set<ControlledJob>>();
		for (CalculationUnitDAG partition : partitionedDAG.getList()) {

			Map<CalculationUnit, Set<ControlledJob>> unitjobs = compiler.compileBasicGFCalculationUnit(partition);
			jobmap.putAll(unitjobs);
		}
		

		setJobDependencies(jobmap);

		Set<ControlledJob> jobs = flattenJobs(jobmap);

		MRPlan plan = new MRPlan();
		plan.addAllJobs(jobs);

		plan.setInputFolder(indir);
		plan.setOutputFolder(outdir);
		plan.setScratchFolder(scratchdir);
		plan.setDeleteTmpDirs(false);
		
		plan.addTempDirs(dm.getTempDirs());
		plan.addOutDirs(dm.getOutDirs());

		return plan;
	}

	/**
	 * @param jobmap
	 * @return
	 */
	private Set<ControlledJob> flattenJobs(Map<CalculationUnit, Set<ControlledJob>> jobmap) {
		Set<ControlledJob> jobs = new HashSet<ControlledJob>();
		
		for (Set<ControlledJob> set : jobmap.values()) {
			jobs.addAll(set);
		}
		
		return jobs;
	}

	/**
	 * @param jobmap
	 */
	private void setJobDependencies(Map<CalculationUnit, Set<ControlledJob>> jobmap) {

		// for each calculation
		for (CalculationUnit cu : jobmap.keySet()) {
			
			// get the dependencies
			Collection<CalculationUnit> dependencies = cu.getDependencies();
			for (CalculationUnit dep : dependencies) {
				
				// get associated MR jobs
				Set<ControlledJob> jobs = jobmap.get(cu);
				Set<ControlledJob> depjobs = jobmap.get(dep);
				
				// set depencencies
				for (ControlledJob depJob : depjobs) {
					for (ControlledJob job : jobs) 
						job.addDependingJob(depJob);
				}
			
			}

		}

	}
}
