/**
 * Created: 2015-06-23
 */
package gumbo.engine.general.grouper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.grouper.structures.CalculationGroup;

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

	Map<Integer, List<CalculationGroup>> semijoins;
	private PartitionedCUGroup partitions;
	
	public GroupedPartitionedCUGroup(PartitionedCUGroup partitions) {
		this.partitions = partitions;
		semijoins = new HashMap<Integer, List<CalculationGroup>>();
	}

	public void setGroup(int level, List<CalculationGroup> groupedSJ) {
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
	public List<CalculationGroup> getSemiJoinGrouping(int level) throws GroupingNotAvailableException {
		
		List<CalculationGroup> result = semijoins.get(level);
		
		if (result == null)
			throw new GroupingNotAvailableException("No grouping found for this level");
		
		return result;
	}


	

}
