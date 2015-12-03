/**
 * Created: 09 May 2014
 */
package gumbo.compiler.linker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.structures.data.RelationSchema;

/**
 * Representation of a CalculationUnit DAG (may or may not be connected). 
 * This class is a wrapper for a set of {@link CalculationUnits}, 
 * which contain their own dependencies. The aim of this class is to provide
 * a collection of operations on the DAG. 
 * 
 * For example, it allows for the discovery of input/output/intermediate
 * relation schemes, which in general cannot be discovered by solely looking at
 * root/leaf calculations. This is the case when an intermediate calculation
 * both depends on another calculation and at the same time needs a raw table.
 * 
 * The structure also allows for easy discovery of
 * leaf (depends on no other calculations) and root (no depending calculations)
 * calculations. 
 * 
 * TODO #core separate schema logic
 * 
 * @author Jonny Daenen
 * 
 */
public class CalculationUnitGroup implements Iterable<CalculationUnit> {

	Set<CalculationUnit> calculations;
	

	public CalculationUnitGroup() {
		calculations = new HashSet<CalculationUnit>();
	}


	public void add(CalculationUnit c) {
		calculations.add(c);

		// TODO check for cyclic dependencies

	}


	public void addAll(CalculationUnitGroup calcSet) {
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
	public CalculationUnitGroup getRoots() {


		CalculationUnitGroup roots = new CalculationUnitGroup();

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
	public CalculationUnitGroup getCalculationsByHeight(int height) {
		CalculationUnitGroup cp = new CalculationUnitGroup();

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
	public CalculationUnitGroup getCalculationsByDepth(int depth) {

		// calculate roots (depth = 1)
		CalculationUnitGroup currentLevel = getRoots();

		// breadth first expansion
		for (int level = 2; level <= depth; level++) {

			CalculationUnitGroup newLevel = new CalculationUnitGroup();

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
		String s = "{" + System.lineSeparator();
		for (CalculationUnit c : calculations) {
			s += c + System.lineSeparator();
		}
		s += "}";

		return s;
	}
	
	public String toShortString() {
		String s = "{";
		for (CalculationUnit c : calculations) {
			s += c.getId() + ",";
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
	 *         calculation TODO what?
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
	 * TODO #core is this "non-output"?
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
	 * Constructs a set containing all calculations.
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


	/**
	 * Constructs the union of dependencies of all calculations.
	 * 
	 * @return the union of all dependencies
	 */
	public Collection<CalculationUnit> getAllDependencies() {
		
		HashSet<CalculationUnit> deps = new HashSet<>();
		
		for (CalculationUnit cu: calculations) {
			deps.addAll(cu.getDependencies());
		}
		
		return deps;
		
	}
	
	
	@Override
	public boolean equals(Object obj) {

		// wrong object
		if (!(obj instanceof CalculationUnitGroup)) {
			return false;
		}
		
		// cast
		CalculationUnitGroup cug = (CalculationUnitGroup) obj;
		
		// wrong size
		if (calculations.size() != cug.calculations.size())
			return false;
			
		// size is ok, so one-way comparison suffices
		for(CalculationUnit cu : cug.calculations) {
			if (!calculations.contains(cu))
				return false;
		}
		
		
		return true;
	}

	@Override
    public int hashCode() {
		int hashcode = 0;
	    for (CalculationUnit cu : calculations) {
	        hashcode ^= cu.hashCode();
	    }
	    return hashcode;
    }


	public String getCanonicalOutString() {
		StringBuffer sb = new StringBuffer();
		for (CalculationUnit cu : calculations) {
			sb.append(cu.getOutputSchema());	
		}
		return sb.toString();
	}

}
