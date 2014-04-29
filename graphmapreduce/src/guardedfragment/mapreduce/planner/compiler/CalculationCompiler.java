/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.compiler;

import guardedfragment.mapreduce.planner.calculations.CalculationPartition;

import java.util.List;

import mapreduce.MRPlan;

/**
 * @author Jonny Daenen
 *
 */
public interface CalculationCompiler {
	
	MRPlan compile(List<CalculationPartition> partitions);

}
