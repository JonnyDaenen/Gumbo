/**
 * Created: 29 Apr 2014
 */
package mapreduce.guardedfragment.planner.partitioner;

import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;

/**
 * Partitions the CalculationUnits based on their height in the DAG.
 * @author Jonny Daenen
 *
 */
public class GreedyPartitioner implements CalculationPartitioner {

	/**
	 * @see mapreduce.guardedfragment.planner.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCalculationUnitDAG partition(CalculationUnitDAG partition) {
		
		int height = partition.getHeight();
		PartitionedCalculationUnitDAG partitionedDAG = new PartitionedCalculationUnitDAG();
		
		// TODO implement
//		for (int i = 1; i <= height; i++) {
//			
//			CalculationUnitDAG calcSet = partition.getCalculationsByHeight(i);
//			partitionedDAG.addToTop(calcSet);
//		}
//		
		
		return partitionedDAG;
	}

}
