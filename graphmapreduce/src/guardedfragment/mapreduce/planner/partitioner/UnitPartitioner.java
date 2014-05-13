/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.partitioner;

import guardedfragment.mapreduce.planner.calculations.CalculationUnit;
import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;

/**
 * Partitions the CalculationUnits in separate partitions. Dependent units will appear later in the list.
 * 
 * @author Jonny Daenen
 *
 */
public class UnitPartitioner implements CalculationPartitioner {

	/**
	 * @see guardedfragment.mapreduce.planner.partitioner.CalculationPartitioner#partition(guardedfragment.mapreduce.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCalculationUnitDAG partition(CalculationUnitDAG partition) {
		
		int height = partition.getHeight();
		PartitionedCalculationUnitDAG partitionedDAG = new PartitionedCalculationUnitDAG();
		
		for (int i = 1; i <= height; i++) {
			
			CalculationUnitDAG calcSet = partition.getCalculationsByHeight(i);
			for (CalculationUnit cu : calcSet) {
				partitionedDAG.add(cu);
			}
			
		}
		
		
		return partitionedDAG;
	}

}
