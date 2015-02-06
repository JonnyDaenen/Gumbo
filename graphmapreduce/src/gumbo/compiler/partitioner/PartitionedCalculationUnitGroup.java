/**
 * Created: 09 May 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Provides level information to {@link CalculationUnit}s in a {@link CalculationUnitGroup},
 * creating a notion of partitions.
 * The partitions are ordered in a bottom-up fashion.
 * 
 * TODO #core maybe add dag as a field?
 * 
 * @author Jonny Daenen
 *
 */
public class PartitionedCalculationUnitGroup extends CalculationUnitGroup {

	Map<CalculationUnit,Integer> levelAssignmment;
	int currentLevel = -1;


	public PartitionedCalculationUnitGroup() {
		levelAssignmment = new HashMap<>();
	}

	/**
	 * Adds a partition as a new level.
	 * @param partition a set of CalculationUnits
	 */
	public void addNewLevel(CalculationUnitGroup partition) {

		// add calculations to the set of all calculations
		super.addAll(partition);

		// add partition
		currentLevel++;
		for (CalculationUnit c : partition) {
			levelAssignmment.put(c, currentLevel);
		}

		// TODO throw exception when dependencies are missing
	}

	/**
	 * Adds a list of CalculationUnits to a given level (specified separately for each one).
	 * The level numbers start at 0.
	 * @param units the CalculationUnits to add
	 * @param levels the levels corresponding to the CalculationUnits
	 */
	public PartitionedCalculationUnitGroup(CalculationUnit [] units, int [] levels) {
		this();

		for (int i = 0; i < units.length; i++) {
			super.add(units[i]);
			levelAssignmment.put(units[i], levels[i]);
			currentLevel = Math.max(currentLevel, levels[i]);
		}
	}

	/**
	 * Adds the calculation unit as a separate partition to the back of the list.
	 * @see gumbo.compiler.linker.CalculationUnitGroup#addnewLevel(gumbo.compiler.calculations.CalculationUnit)
	 */
	@Override
	public void add(CalculationUnit c) {
		CalculationUnitGroup unitDAG = new CalculationUnitGroup();
		unitDAG.add(c);
		addNewLevel(unitDAG);
	}


	/**
	 * @return a list of partitions of CalculationsUnits, ordered by their level (small to large)
	 */
	public List<CalculationUnitGroup> getBottomUpList() {
		LinkedList<CalculationUnitGroup> l = new LinkedList<>();

		for (int i = 0; i <= currentLevel; i++) {
			CalculationUnitGroup set = new CalculationUnitGroup();
			for (CalculationUnit c : levelAssignmment.keySet()) {
				if (levelAssignmment.get(c) == i)
					set.add(c);
			}
			l.add(set);
		}

		return l;
	}

	public int getNumPartitions() {
		return currentLevel+1;
	}


	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "Calculation Unit Partitions: {" + System.lineSeparator();
		for (int i = 0; i <= currentLevel; i++) {
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
	 * @return whether the partition retains the dependencies.
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
	 * Checks if all levels contains at least one calculation unit.
	 * @return true iff all indicated levels are occupied
	 */
	public boolean allLevelsUsed() {
		for (int i = 0; i <= currentLevel; i++) {
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
	 * number of <b>different</b> levels a dependency appears on.
	 * 
	 * @param c a calculation unit
	 * @return the spread of the calculationunit
	 */
	public int getSpread(CalculationUnit c) {
		boolean [] levels = new boolean[currentLevel+1];

		// for each possible parent
		for (CalculationUnit p : getCalculations()) {

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


}
