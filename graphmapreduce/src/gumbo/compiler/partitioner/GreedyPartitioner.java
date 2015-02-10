/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.resolver.DirManager;

/**
 * Partitions the CalculationUnits based on their height in the DAG.
 * @author Jonny Daenen
 *
 */
public class GreedyPartitioner implements CalculationPartitioner {

	/**
	 * @see gumbo.compiler.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCUGroup partition(CalculationUnitGroup partition, FileManager fm) {
		
		int height = partition.getHeight();
		PartitionedCUGroup partitionedDAG = new PartitionedCUGroup();
		
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
