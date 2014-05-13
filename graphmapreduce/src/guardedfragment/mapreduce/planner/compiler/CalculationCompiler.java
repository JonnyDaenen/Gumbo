/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.compiler;

import org.apache.hadoop.fs.Path;

import guardedfragment.mapreduce.planner.partitioner.PartitionedCalculationUnitDAG;
import mapreduce.MRPlan;

/**
 * @author Jonny Daenen
 *
 */
public interface CalculationCompiler {
	
	MRPlan compile(PartitionedCalculationUnitDAG partitionedDAG, Path indir, Path outdir, Path scratchdir) throws UnsupportedCalculationUnitException, CompilerException;

}
