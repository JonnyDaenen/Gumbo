/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.partitioner;

import guardedfragment.mapreduce.planner.calculations.CalculationUnit;
import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Jonny Daenen
 *
 */
public class HeightPartitioner implements CalculationPartitioner {

	/**
	 * @see guardedfragment.mapreduce.planner.partitioner.CalculationPartitioner#partition(guardedfragment.mapreduce.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCalculationUnitDAG partition(CalculationUnitDAG partition) {
		
		int height = partition.getHeight();
		PartitionedCalculationUnitDAG partitionedDAG = new PartitionedCalculationUnitDAG();
		
		for (int i = 1; i <= height; i++) {
			
			CalculationUnitDAG calcSet = partition.getCalculationsByHeight(i);
			partitionedDAG.add(calcSet);
		}
		
		
		return partitionedDAG;
	}

}
