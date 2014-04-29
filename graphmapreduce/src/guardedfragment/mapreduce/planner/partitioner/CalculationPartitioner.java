/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.partitioner;

import guardedfragment.mapreduce.planner.calculations.CalculationPartition;
import guardedfragment.mapreduce.planner.calculations.CalculationUnit;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Divides a set of CalculationUnits into a a sequence of partitions,
 * based on their dependencies
 * 
 * @author Jonny Daenen
 *
 */
public interface CalculationPartitioner {


	List<CalculationPartition> partition(CalculationPartition calculationSet);

}
