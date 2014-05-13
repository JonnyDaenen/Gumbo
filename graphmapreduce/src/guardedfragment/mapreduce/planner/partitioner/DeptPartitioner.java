/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.partitioner;

import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;

/**
 * Partitions the CalculationUnits based on their height in the DAG.
 * @author Jonny Daenen
 *
 */
public class DeptPartitioner implements CalculationPartitioner {

	/**
	 * @see guardedfragment.mapreduce.planner.partitioner.CalculationPartitioner#partition(guardedfragment.mapreduce.planner.calculations.CalculationPartition)
	 */
	@Deprecated
	@Override
	public PartitionedCalculationUnitDAG partition(CalculationUnitDAG partition) {
		
		// TODO implement
		int height = partition.getHeight();
		PartitionedCalculationUnitDAG partitionedDAG = new PartitionedCalculationUnitDAG();
		
		for (int i = 1; i <= height; i++) {
			
			CalculationUnitDAG calcSet = partition.getCalculationsByHeight(i);
			partitionedDAG.add(calcSet);
		}
		
		
		return partitionedDAG;
	}

}
