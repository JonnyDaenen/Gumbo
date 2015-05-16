/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.linker.CalculationUnitGroup;

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
	public PartitionedCUGroup partition(CalculationUnitGroup partition, FileManager fm) {
		
		int height = partition.getHeight();
		PartitionedCUGroup partitionedDAG = new PartitionedCUGroup();
		
		for (int i = 1; i <= height; i++) {
			
			CalculationUnitGroup calcSet = partition.getCalculationsByHeight(i);
			for (CalculationUnit cu : calcSet) {
				partitionedDAG.addNewLevel(cu);
			}
			
		}
		
		
		return partitionedDAG;
	}

}
