/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.partitioner;

import guardedfragment.mapreduce.planner.calculations.CalculationPartition;
import guardedfragment.mapreduce.planner.calculations.CalculationUnit;

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
	public List<CalculationPartition> partition(CalculationPartition partition) {
		
		int height = partition.getHeight();
		
		ArrayList<CalculationPartition> list = new ArrayList<CalculationPartition>(height+1);
		
		for (int i = 0; i <= height; i++) {
			
			CalculationPartition calcSet = partition.getCalculationsByHeight(height);
			list.add(i, calcSet);
		}
		
		
		return list;
	}

}
