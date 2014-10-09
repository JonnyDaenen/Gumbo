/**
 * Created: 29 Apr 2014
 */
package mapreduce.guardedfragment.planner.partitioner;

import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;

/**
 * Partitions the CalculationUnits based on their depth in the DAG.
 * @author Jonny Daenen
 *
 */
public class DepthPartitioner implements CalculationPartitioner {

	/**
	 * @see mapreduce.guardedfragment.planner.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCalculationUnitDAG partition(CalculationUnitDAG partition) {

		int height = partition.getHeight();
		PartitionedCalculationUnitDAG partitionedDAG = new PartitionedCalculationUnitDAG();
		
		for (int i = height; i > 0; i--) {
			
			// OPTIMIZE this is rather inefficient as each time a DSF from top is done
			CalculationUnitDAG calcSet = partition.getCalculationsByDepth(i);

			partitionedDAG.addToTop(calcSet);
		}
		
		
		return partitionedDAG;
	}

}
