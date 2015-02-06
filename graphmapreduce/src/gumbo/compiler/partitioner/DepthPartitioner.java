/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.resolver.DirManager;

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
	public PartitionedCalculationUnitGroup partition(CalculationUnitGroup partition, DirManager dm) {

		int height = partition.getHeight();
		PartitionedCalculationUnitGroup partitionedDAG = new PartitionedCalculationUnitGroup();
		
		for (int i = height; i > 0; i--) {
			
			// OPTIMIZE this is rather inefficient as each time a DSF from top is done
			CalculationUnitGroup calcSet = partition.getCalculationsByDepth(i);

			partitionedDAG.addNewLevel(calcSet);
		}
		
		
		return partitionedDAG;
	}

}
