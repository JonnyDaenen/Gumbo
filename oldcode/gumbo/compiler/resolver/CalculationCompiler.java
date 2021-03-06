/**
 * Created: 09 May 2014
 */
package gumbo.compiler.resolver;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.compiler.resolver.operations.MRJob;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

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
	 * Creates a {@link MRPlan} that will execute the given calculations.
	 * 
	 * 
	 * @param partitionedDAG
	 *            the partitioned DAG of calculation units
	 * @param infiles
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
	 * @see gumbo.compiler.resolver.CalculationCompiler#compile(java.util.List)
	 */
	public MRPlan compile(PartitionedCUGroup partitionedDAG, RelationFileMapping infiles, Path outdir, Path scratchdir)
			throws UnsupportedCalculationUnitException, CompilerException {

		counter = 0;

		// for each partition we create a set of MR-jobs

		// map input and output relations to files
		dm = new DirManager(partitionedDAG, infiles, outdir, scratchdir);
		
		// TODO global serializer
		// TODO change job name
		BasicGFCompiler compiler = new BasicGFCompiler(dm, new GFPrefixSerializer(), "Fronjo_job");


		// convert all to MR-jobs and set INTRA-calculation dependencies
		Map<CalculationUnit, Set<MRJob>> jobmap = new HashMap<CalculationUnit, Set<MRJob>>();
		for (CalculationUnitGroup partition : partitionedDAG.getBottomUpList()) {

			Map<CalculationUnit, Set<MRJob>> unitjobs = compiler.compileBasicGFCalculationUnit(partition);
			jobmap.putAll(unitjobs);
		}
		
		// set INTER-calculation dependencies
		setJobDependencies(jobmap);
		
		

		Set<MRJob> jobs = flattenJobs(jobmap);
		
		// remove indirect dependencies
		for (MRJob job : jobs) {
			job.removeUnnecessaryDependencies();
		}

		MRPlan plan = new MRPlan();
		plan.addAllJobs(jobs);

		plan.setInputPaths(infiles);
		plan.setOutputFolder(outdir);
		plan.setScratchFolder(scratchdir);
		plan.setDeleteTmpDirs(false);
		
		plan.addTempDirs(dm.getTempDirs());
		plan.addOutDirs(dm.getOutDirs());
		
		plan.setDirManager(dm);

		return plan;
	}

	/**
	 * @param jobmap
	 * @return
	 */
	private Set<MRJob> flattenJobs(Map<CalculationUnit, Set<MRJob>> jobmap) {
		Set<MRJob> jobs = new HashSet<MRJob>();
		
		for (Set<MRJob> set : jobmap.values()) {
			jobs.addAll(set);
		}
		
		return jobs;
	}

	/**
	 * @param jobmap
	 */
	private void setJobDependencies(Map<CalculationUnit, Set<MRJob>> jobmap) {

		// for each calculation
		for (CalculationUnit cu : jobmap.keySet()) {
			
			// get the dependencies
			Collection<CalculationUnit> dependencies = cu.getDependencies();
			for (CalculationUnit dep : dependencies) {
				
				// get associated MR jobs
				Set<MRJob> jobs = jobmap.get(cu);
				Set<MRJob> depjobs = jobmap.get(dep);
				
				// set depencencies
				for (MRJob depJob : depjobs) {
					for (MRJob job : jobs) 
						job.addDependingJob(depJob);
				}
			
			}

		}

	}
}
