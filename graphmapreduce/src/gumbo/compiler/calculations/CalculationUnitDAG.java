/**
 * Created: 09 May 2014
 */
package gumbo.compiler.calculations;

import gumbo.compiler.structures.data.RelationSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Representation of a (set of) CalculationUnit DAG(s). The DAG indicates which
 * calculations depend on others. The structure allows for easy discovery of
 * leaf (depends on no other calculations) and root (no depending calculations)
 * calculations. It also allows for the discovery of input/output/intermediate
 * relation schemes, which in general cannot be discovered by solely looking at
 * root/leaf calculations. This is the case when an intermediate calculation
 * both depends on another calculation and at the same time needs a raw table.
 * 
 * 
 * @author Jonny Daenen
 * 
 *         TODO write testcode
 */
public class CalculationUnitDAG implements Iterable<CalculationUnit> {

	Set<CalculationUnit> calculations;


	public CalculationUnitDAG() {
		calculations = new HashSet<CalculationUnit>();
	}


	public void add(CalculationUnit c) {
		calculations.add(c);

		// TODO check for cyclic dependencies

	}


	public void addAll(CalculationUnitDAG calcSet) {
		for (CalculationUnit cu : calcSet) {
			calculations.add(cu);
			// TODO check for cyclic dependencies
		}
	}


	/**
	 * Calculates the set of independent calculations.
	 * 
	 * @return set of CalculationsUnits that are not dependent of any other
	 *         calculations
	 */
	public Collection<CalculationUnit> getLeafs() {
		ArrayList<CalculationUnit> leafs = new ArrayList<CalculationUnit>();
		for (CalculationUnit c : calculations) {
			if (c.isLeaf())
				leafs.add(c);

		}
		return leafs;
	}

	/**
	 * Calculates the set of calculations on which no others depend.
	 * 
	 * @return set of CalculationsUnits on which no others depend
	 */
	public CalculationUnitDAG getRoots() {


		CalculationUnitDAG roots = new CalculationUnitDAG();

		// add to root set if applicable
		for (CalculationUnit currentCalc : calculations) {

			boolean isRoot = true;
			for (CalculationUnit oldCalc : calculations) {

				if (oldCalc.getDependencies().contains(currentCalc)) {
					isRoot = false;
					break;
				}
			}

			if (isRoot) {
				roots.add(currentCalc);
			}
		}

		return roots;
	}

	/**
	 * Calculates the maximum height of the DAG(s). The entire set of calculations is traversed.
	 * @return the maximum height of the DAG(s)
	 */
	public int getHeight() {
		int max = 0;
		for (CalculationUnit c : calculations) {
			max = Math.max(max, c.getHeight());
		}

		return max;

	}

	/**
	 * TODO levelwise?
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<CalculationUnit> iterator() {
		return calculations.iterator();
	}

	/**
	 * @param height
	 * @return partition with all calculations of the specified height
	 */
	public CalculationUnitDAG getCalculationsByHeight(int height) {
		CalculationUnitDAG cp = new CalculationUnitDAG();

		for (CalculationUnit c : calculations) {
			if (c.getHeight() == height)
				cp.add(c);
		}

		return cp;
	}

	/**
	 * Calculates the set of calculation units on a specified depth
	 * (depth of root calculation units = 1).
	 * 
	 * @param depth
	 * @return partition with all calculations of the specified depth
	 */
	public CalculationUnitDAG getCalculationsByDepth(int depth) {

		// calculate roots (depth = 1)
		CalculationUnitDAG currentLevel = getRoots();

		// breadth first expansion
		for (int level = 2; level <= depth; level++) {

			CalculationUnitDAG newLevel = new CalculationUnitDAG();

			// for each current cu
			for (CalculationUnit c : currentLevel) {

				// add all the children to the new level
				for (CalculationUnit child : c.getDependencies()) {
					newLevel.add(child);

				}
			}

			// level complete -> shift
			currentLevel = newLevel;

		}

		// return last calculated level
		return currentLevel;
	}

	/**
	 * @return
	 */
	public int size() {
		return calculations.size();
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "Calculation Unit Set: {" + System.lineSeparator();
		for (CalculationUnit c : calculations) {
			s += c + System.lineSeparator();
		}
		s += "}";

		return s;
	}


	/**
	 * @return the set of relations that appear as output schema
	 */
	protected Set<RelationSchema> getAllOutputRelations() {
		Set<RelationSchema> out = new HashSet<RelationSchema>();
		for (CalculationUnit cu : calculations) {
			out.add(cu.getOutputSchema());
		}

		return out;
	}

	/**
	 * @return the set of relations that appear as input schema
	 */
	protected Set<RelationSchema> getAllInputRelations() {
		Set<RelationSchema> out = new HashSet<RelationSchema>();
		for (CalculationUnit cu : calculations) {
			out.addAll(cu.getInputRelations());
		}


		return out;
	}

	/**
	 * @return the set of relations that appear as input or output schema
	 */
	protected Set<RelationSchema> getAllRelations() {
		Set<RelationSchema> out = new HashSet<RelationSchema>();
		out.addAll(getAllInputRelations());
		out.addAll(getAllOutputRelations());

		return out;
	}

	/**
	 * @return the set of relations that cannot be obtained using a dependent
	 *         calculation
	 */
	public Set<RelationSchema> getInputRelations() {
		Set<RelationSchema> in = new HashSet<RelationSchema>();
		in.addAll(getAllInputRelations());
		in.removeAll(getAllOutputRelations());
		return in;
	}

	/**
	 * @return the set of relations that cannot be obtained using a dependent
	 *         calculation
	 */
	public Set<RelationSchema> getOutputRelations() {
		Set<RelationSchema> out = new HashSet<RelationSchema>();
		out.addAll(getAllOutputRelations());
		out.removeAll(getAllInputRelations());

		return out;
	}

	/**
	 * Calculates the set of relations that will be calculated by calculations
	 * units.
	 * 
	 * @return the set of non-output relations
	 */
	public Set<RelationSchema> getIntermediateRelations() {
		Set<RelationSchema> temp = new HashSet<RelationSchema>();
		temp.addAll(getAllRelations());
		temp.removeAll(getInputRelations());
		temp.removeAll(getOutputRelations());
		return temp;
	}


	/**
	 * @return the set of calculations
	 */
	public Set<CalculationUnit> getCalculations() {
		Set<CalculationUnit> out = new HashSet<CalculationUnit>();
		out.addAll(calculations);
		return out;

	}


	/**
	 * @param c
	 * @return
	 */
	public int getDepth(CalculationUnit c) {
		// roots have depth 1
		int maxDepth = 1;
		for (CalculationUnit p : getCalculations()) {

			// is parent?
			if (p.getDependencies().contains(c)) {
				maxDepth = Math.max(maxDepth, getDepth(p) + 1);
			}
		}
		return maxDepth;

	}
}
