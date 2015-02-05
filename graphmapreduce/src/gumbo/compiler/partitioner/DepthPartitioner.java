/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.calculations.CalculationUnitDAG;

/**
 * Partitions the CalculationUnits based on their depth in the DAG.
 * @author Jonny Daenen
 *
 */
public class DepthPartitioner implements CalculationPartitioner {

	/**
	 * @see gumbo.compiler.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCalculationUnitDAG partition(CalculationUnitDAG partition) {

		int height = partition.getHeight();
		PartitionedCalculationUnitDAG partitionedDAG = new PartitionedCalculationUnitDAG();
		
		for (int i = height; i > 0; i--) {
			
			// OPTIMIZE this is rather inefficient as each time a DSF from top is done
			CalculationUnitDAG calcSet = partition.getCalculationsByDepth(i);

			partitionedDAG.addNewLevel(calcSet);
		}
		
		
		return partitionedDAG;
	}

}
