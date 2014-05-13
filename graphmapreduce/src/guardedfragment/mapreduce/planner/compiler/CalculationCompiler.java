/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.compiler;

import guardedfragment.mapreduce.planner.partitioner.PartitionedCalculationUnitDAG;
import mapreduce.MRPlan;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 *
 */
public interface CalculationCompiler {
	
	MRPlan compile(PartitionedCalculationUnitDAG partitionedDAG, Path indir, Path outdir, Path scratchdir) throws UnsupportedCalculationUnitException, CompilerException;

}
