/**
 * Created: 09 May 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Adds level information to {@link CalculationUnit}s in a {@link CalculationUnitGroup},
 * creating partitions.
 * The partitions are ordered in a bottom-up fashion, 0 being the first.
 * 
 * TODO replace level wording with partition
 * 
 * @author Jonny Daenen
 *
 */
public class PartitionedCUGroup {

	protected CalculationUnitGroup group;
	
	protected Map<CalculationUnit, Integer> levelAssignmment; // FUTURE convert to list?
	// FUTURE add reverse mapping
	protected int currentPartition = -1;


	public PartitionedCUGroup() {
		levelAssignmment = new HashMap<>();
		group = new CalculationUnitGroup();
	}

	/**
	 * Adds a list of CalculationUnits to a given level (specified separately for each one).
	 * The level numbers start at 0.
	 * @param units the CalculationUnits to add
	 * @param levels the levels corresponding to the CalculationUnits
	 */
	public PartitionedCUGroup(CalculationUnit [] units, int [] levels) {
		this();

		for (int i = 0; i < units.length; i++) {
			group.add(units[i]);
			levelAssignmment.put(units[i], levels[i]);
			currentPartition = Math.max(currentPartition, levels[i]);
		}
	}
	

	/**
	 * Adds a partition as a new level.
	 * @param partition a set of CalculationUnits
	 */
	public void addNewLevel(CalculationUnitGroup partition) {

		// add calculations to the set of all calculations
		group.addAll(partition);

		// add partition
		currentPartition++;
		for (CalculationUnit c : partition) {
			levelAssignmment.put(c,currentPartition);
		}

		// TODO throw exception when dependencies are missing
	}


	/**
	 * Adds the calculation unit as a new, separate partition to the back of the list.
	 * @see gumbo.compiler.linker.CalculationUnitGroup#addnewLevel(gumbo.compiler.calculations.CalculationUnit)
	 */
	public void addNewLevel(CalculationUnit c) {
		group.add(c);
		levelAssignmment.put(c,++currentPartition);
	}


	/**
	 * Creates a list of {@link CalculationUnitGroup}s, corresponding to th differnt partitions.
	 * The partitions are ordered by their level: small/bottom to large/top.
	 * 
	 * @return a list of partitions ordered by their level (small to large)
	 */
	public List<CalculationUnitGroup> getBottomUpList() {
		LinkedList<CalculationUnitGroup> l = new LinkedList<>();

		for (int i = 0; i <= currentPartition; i++) {
			CalculationUnitGroup set = new CalculationUnitGroup();
			for (CalculationUnit c : levelAssignmment.keySet()) {
				if (levelAssignmment.get(c) == i)
					set.add(c);
			}
			l.add(set);
		}

		return l;
	}
	
	/**
	 * Constructs (!) a {@link CalculationUnitGroup} that contains precisely
	 * the {@link CalculationUnit}s of the specified level.
	 * 
	 * @param level the requested level
	 * 
	 * @return the {@link CalculationUnit}s of the specified level
	 */
	public CalculationUnitGroup getPartition(int i) {
		CalculationUnitGroup set = new CalculationUnitGroup();
		for (CalculationUnit c : levelAssignmment.keySet()) {
			if (levelAssignmment.get(c) == i)
				set.add(c);
		}
		return set;
	}

	
	/**
	 * The number of partitions.
	 * @return the number of partitions
	 */
	public int getNumPartitions() {
		return currentPartition+1;
	}


	/**
	 * Constructs a representation of the partition.
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "Calculation Unit Partitions: {" + System.lineSeparator();
		for (int i = 0; i <= currentPartition; i++) {
			s += "{";
			for (CalculationUnit c : levelAssignmment.keySet()) {
				if (levelAssignmment.get(c) == i)
					s += c;
			}
			s += "}" + System.lineSeparator();
		}
		s += "}";

		return s;
	}

	/**
	 * Checks whether the partition respects the dependencies between 
	 * the contained {@link CalculationUnit}s.
	 * 
	 * @return whether the partition respects the dependencies
	 */
	public boolean checkDependencies() {

		// for each calculation unit
		for (CalculationUnit c : levelAssignmment.keySet()) {

			// check dependencies
			for (CalculationUnit d : c.getDependencies()) {

				// dependencies should be on a lower level
				if (levelAssignmment.get(c) <= levelAssignmment.get(d))
					return false;
			}

		}

		// no errors were found
		return true;
	}
	

	/**
	 * Returns whether all levels contain at least one calculation unit.
	 * @return <code>true</code> when every level in the partition is occupied, <code>false</code> otherwise
	 */
	public boolean allLevelsUsed() {
		for (int i = 0; i <= currentPartition; i++) {
			// count number on this level
			int counter = 0;
			for (CalculationUnit c : levelAssignmment.keySet()) {
				if (levelAssignmment.get(c) == i)
					counter++;
			}
			// error if none are present
			if (counter == 0)
				return false;
		}

		// no bad levels found
		return true;
	}

	/**
	 * Calculates the spread of the CU, i.e., the
	 * number of <b>different</b> levels a parent appears on.
	 * In practice, this is useful to determine e.g. on how many
	 * levels this {@link CalculationUnit} is read/accessed.
	 * 
	 * 
	 * @param c a calculation unit
	 * @return the spread of the calculationunit
	 */
	public int getSpread(CalculationUnit c) {
		boolean [] levels = new boolean[currentPartition+1];

		// for each possible parent
		for (CalculationUnit p : group) {

			// if it is a parent
			if (p.getDependencies().contains(c)) {

				// keep track of level
				Integer level = levelAssignmment.get(p);
				if (level != null)
					levels[level] = true;
			}
		}

		// count used levels
		int counter = 0;
		for (int i = 0; i < levels.length; i++) {
			if (levels[i])
				counter++;
		}

		return counter;
	}
	
	/**
	 * TODO make it a copy?
	 * @return the collection of {@link CalculationUnit}s
	 */
	public CalculationUnitGroup getCalculationUnits() {
		return group;
	}

	/**
	 * Constructs the set of partitions that each contain at least on of the dependencies
	 * of a calculation in the given group.
	 * 
	 * @param cug a group of calculations
	 * 
	 * @return the set of partitions that contain at least one of the dependencies
	 */
	public Collection<CalculationUnitGroup> getDependentPartitions(CalculationUnitGroup cug) {
		HashSet<CalculationUnitGroup> depParts = new HashSet<>();
		Collection<CalculationUnit> deps = cug.getAllDependencies();
		
		// for each partition
		for (int i = 0; i <= currentPartition; i++) {
			CalculationUnitGroup current = getPartition(i);
			
			// check if it contains dependencies
			Set<CalculationUnit> calcs = current.getCalculations();
			calcs.retainAll(deps);
			if (calcs.size() != 0) {
				// add partition if there are
				depParts.add(current); 
			}
		}
		
		return depParts;
	}


}
