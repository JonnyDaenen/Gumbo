/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.linker.CalculationUnitGroup;

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
	public PartitionedCUGroup partition(CalculationUnitGroup partition, FileManager fm) {

		int height = partition.getHeight();
		PartitionedCUGroup partitionedDAG = new PartitionedCUGroup();
		
		for (int i = height; i > 0; i--) {
			
			// OPTIMIZE this is rather inefficient as each time a DSF from top is done
			CalculationUnitGroup calcSet = partition.getCalculationsByDepth(i);

			partitionedDAG.addNewLevel(calcSet);
		}
		
		
		return partitionedDAG;
	}

}
