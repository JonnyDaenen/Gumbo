/**
 * Created: 2015-06-23
 */
package gumbo.compiler.grouper;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link PartitionedCUGroup}, augmented with a set of smaller
 * calculationsets. These calculationsets indicate how the grouping
 * of the jobs should be done .
 * 
 * @author Jonny Daenen
 *
 */
public class GroupedPartitionedCUGroup {

	public class GroupingNotAvailableException extends Exception {
		private static final long serialVersionUID = 1L;

		public GroupingNotAvailableException(String msg) {
			super(msg);
		}

	}

	Map<Integer, List<Set<GuardedSemiJoinCalculation>>> semijoins;
	private PartitionedCUGroup partitions;
	
	public GroupedPartitionedCUGroup(PartitionedCUGroup partitions) {
		this.partitions = partitions;
		semijoins = new HashMap<Integer, List<Set<GuardedSemiJoinCalculation>>>();
	}

	public void setGroup(int level, List<Set<GuardedSemiJoinCalculation>> groupedSJ) {
		semijoins.put(level, groupedSJ);
	}
	
	public PartitionedCUGroup getPartitions() {
		return partitions;
	}
	
	/**
	 * Returns the semijoin grouping for the specified level.
	 * 
	 * @param level the level for wich to fetch the semijoin grouping
	 * @return a semijoin grouping
	 * 
	 * @throws GroupingNotAvailableException when no grouping is attached to the specified level
	 */
	public List<Set<GuardedSemiJoinCalculation>> getSemiJoinGrouping(int level) throws GroupingNotAvailableException {
		
		List<Set<GuardedSemiJoinCalculation>> result = semijoins.get(level);
		
		if (result == null)
			throw new GroupingNotAvailableException("No grouping found for this level");
		
		return result;
	}


	

}
