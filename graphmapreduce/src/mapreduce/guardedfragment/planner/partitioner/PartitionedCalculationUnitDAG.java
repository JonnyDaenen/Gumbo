/**
 * Created: 09 May 2014
 */
package mapreduce.guardedfragment.planner.partitioner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

	Map<CalculationUnit,Integer> levelAssignmment;
	int currentLevel = -1;


	public PartitionedCalculationUnitDAG() {
		levelAssignmment = new HashMap<>();
	}

	/**
	 * Adds a partition as a new level.
	 * @param partition a set of CalculationUnits
	 */
	public void addNewLevel(CalculationUnitDAG partition) {

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
	public PartitionedCalculationUnitDAG(CalculationUnit [] units, int [] levels) {
		this();

		for (int i = 0; i < units.length; i++) {
			super.add(units[i]);
			levelAssignmment.put(units[i], levels[i]);
			currentLevel = Math.max(currentLevel, levels[i]);
		}
	}

	/**
	 * Adds the calculation unit as a separate partition to the back of the list.
	 * @see mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG#addnewLevel(mapreduce.guardedfragment.planner.calculations.CalculationUnit)
	 */
	@Override
	public void add(CalculationUnit c) {
		CalculationUnitDAG unitDAG = new CalculationUnitDAG();
		unitDAG.add(c);
		addNewLevel(unitDAG);
	}


	/**
	 * @return a list of partitions of CalculationsUnits, ordered by their level (small to large)
	 */
	public List<CalculationUnitDAG> getBottomUpList() {
		LinkedList<CalculationUnitDAG> l = new LinkedList<>();

		for (int i = 0; i <= currentLevel; i++) {
			CalculationUnitDAG set = new CalculationUnitDAG();
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
