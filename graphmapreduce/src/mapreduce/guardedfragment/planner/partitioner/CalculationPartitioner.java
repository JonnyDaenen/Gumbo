/**
 * Created: 29 Apr 2014
 */
package mapreduce.guardedfragment.planner.partitioner;

import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;

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
