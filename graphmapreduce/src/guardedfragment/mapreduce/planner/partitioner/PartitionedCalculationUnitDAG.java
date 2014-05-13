/**
 * Created: 09 May 2014
 */
package guardedfragment.mapreduce.planner.partitioner;

import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Representation for a list of partitions of calculation units.
 * It provides several benefits compared to a raw list.
 * 
 * @author Jonny Daenen
 *
 */
public class PartitionedCalculationUnitDAG extends CalculationUnitDAG {
	
	List<CalculationUnitDAG> partitions;
	
	
	public PartitionedCalculationUnitDAG() {
		partitions = new LinkedList<CalculationUnitDAG>();
	}
	
	/**
	 * Adds a partition to the back of the list.
	 * @param p the partition to add
	 */
	public void add(CalculationUnitDAG partition) {
		
		// add calculations to the set of all calculations
		super.addAll(partition);
		
		// add partition
		partitions.add(partition);
	}


	/**
	 * @return an unmodifiable list of partitions of CalculationsUnits
	 */
	public List<CalculationUnitDAG> getList() {
		return Collections.unmodifiableList(partitions);
	}
	
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "Calculation Unit Partitions: {" + System.lineSeparator();
		for (CalculationUnitDAG c : partitions) {
			s += c + System.lineSeparator();
		}
		s += "}";

		return s;
	}
	

}
