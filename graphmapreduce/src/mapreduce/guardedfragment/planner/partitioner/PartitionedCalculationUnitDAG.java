/**
 * Created: 09 May 2014
 */
package mapreduce.guardedfragment.planner.partitioner;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import mapreduce.guardedfragment.planner.calculations.CalculationUnit;
import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;

/**
 * Representation for a list of partitions of calculation units.
 * It provides several benefits compared to a raw list.
 * The partitions are ordered in a bottom-up fashion.
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
	public void addToTop(CalculationUnitDAG partition) {
		
		// add calculations to the set of all calculations
		super.addAll(partition);
		
		// add partition
		partitions.add(partition);
		
		// TODO throw exception when dependencies are missing
	}
	
	 /**
	  * Adds the calculation unit as a separate partition to the back of the list.
	 * @see mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG#addToTop(mapreduce.guardedfragment.planner.calculations.CalculationUnit)
	 */
	@Override
	public void addToTop(CalculationUnit c) {
		CalculationUnitDAG unitDAG = new CalculationUnitDAG();
		unitDAG.addToTop(c);
		addToTop(unitDAG);
	}


	/**
	 * @return an unmodifiable list of partitions of CalculationsUnits
	 */
	public List<CalculationUnitDAG> getBottomUpList() {
		return Collections.unmodifiableList(partitions);
	}
	
	public int getNumPartitions() {
		return partitions.size();
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
