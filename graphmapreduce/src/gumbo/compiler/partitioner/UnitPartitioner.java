/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.calculations.CalculationUnitDAG;

/**
 * Partitions the CalculationUnits in separate partitions. Dependent units will appear later in the list.
 * 
 * @author Jonny Daenen
 *
 */
public class UnitPartitioner implements CalculationPartitioner {

	/**
	 * @see gumbo.compiler.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
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
