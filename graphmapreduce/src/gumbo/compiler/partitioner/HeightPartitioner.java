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
public class HeightPartitioner implements CalculationPartitioner {

	/**
	 * @see gumbo.compiler.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCalculationUnitGroup partition(CalculationUnitGroup partition, FileManager fm) {
		
		int height = partition.getHeight();
		PartitionedCalculationUnitGroup partitionedDAG = new PartitionedCalculationUnitGroup();
		
		for (int i = 1; i <= height; i++) {
			
			CalculationUnitGroup calcSet = partition.getCalculationsByHeight(i);
			partitionedDAG.addNewLevel(calcSet);
		}
		
		
		return partitionedDAG;
	}

}
