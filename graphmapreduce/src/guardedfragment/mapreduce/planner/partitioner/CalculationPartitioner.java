/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.partitioner;

import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;

/**
 * Divides a set of CalculationUnits into a a sequence of partitions,
 * based on their dependencies
 * 
 * @author Jonny Daenen
 *
 */
public interface CalculationPartitioner {


	PartitionedCalculationUnitDAG partition(CalculationUnitDAG calculations);

}
